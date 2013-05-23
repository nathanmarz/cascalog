(ns cascalog.rules
  (:use [cascalog.debug :only (debug-print)]
        [clojure.set :only (intersection union difference subset?)]
        [jackknife.core :only (throw-illegal throw-runtime)])
  (:require [jackknife.seq :as s]
            [cascalog.vars :as v]
            [cascalog.util :as u]
            [cascalog.options :as opts]
            [cascalog.graph :as g]
            [cascalog.parse :as parse]
            [cascalog.predicate :as p]
            [cascalog.predmacro :as predmacro]
            [cascalog.fluent.tap :as tap]
            [cascalog.fluent.workflow :as w])
  (:import [cascading.tap Tap]
           [cascading.tuple Fields Tuple TupleEntry]
           [cascading.flow Flow FlowConnector]
           [cascading.pipe Pipe]
           [cascading.flow.hadoop HadoopFlowProcess]
           [cascading.pipe.joiner CascalogJoiner CascalogJoiner$JoinType]
           [cascalog.aggregator CombinerSpec]
           [cascalog ClojureCombiner ClojureCombinedAggregator Util
            ClojureParallelAgg]
           [org.apache.hadoop.mapred JobConf]
           [jcascalog Predicate Subquery PredicateMacro ClojureOp
            PredicateMacroTemplate]
           [java.util ArrayList]))

;; infields for a join are the names of the join fields
(p/defpredicate join :infields)
(p/defpredicate group
  :assembly
  :infields
  :totaloutfields)

(defn- find-generator-join-set-vars [node]
  (let [predicate     (g/get-value node)
        inbound-nodes (g/get-inbound-nodes node)
        pred-type     (:type predicate)]
    (condp = pred-type
      :join       nil
      :group      nil
      :generator (if-let [v (:join-set-var predicate)] [v])
      (if (= 1 (count inbound-nodes))
        (recur (first inbound-nodes))
        (throw-runtime "Planner exception: Unexpected number of "
                       "inbound nodes to non-generator predicate.")))))

(defstruct tailstruct
  :ground? ;; Does this thing have any hanging ungrounding vars?
  :operations ;; Operations that potentially could be applied
  :drift-map ;; map of variables -> equalities
  :available-fields ;; fields presented by the tail.
  :node)

;; ## Operation Application

(letfn [
        ;; The assumption here is that the new operation is only
        ;; ADDING fields. All output fields are unique, in this
        ;; particular case.
        ;;
        ;; We have a bug here, because input fields show up before
        ;; output fields -- if we write a certain input field BEFORE
        ;; an output field, we lose.
        (connect-op [tail op]
          (struct tailstruct
                  (:ground? tail)
                  (:operations tail)
                  (:drift-map tail)
                  (concat (:available-fields tail)
                          (:outfields op))
                  (g/connect-value (:node tail) op)))

        ;; Adds an operation onto the tail -- this builds up an entry
        ;; in the graph by connecting the new operation to the old
        ;; operation's node. This is how we keep track of everything.
        (add-op [tail op]
          (debug-print "Adding op to tail " op tail)
          (let [tail    (connect-op tail op)
                new-ops (s/remove-first (partial = op) (:operations tail))]
            (merge tail {:operations new-ops})))

        (op-allowed? [tail op]
          (let [ground?          (:ground? tail)
                available-fields (:available-fields tail)
                join-set-vars    (find-generator-join-set-vars (:node tail))
                infields-set     (set (:infields op))]
            (and (or (:allow-on-genfilter? op)
                     (empty? join-set-vars))
                 (subset? infields-set (set available-fields))
                 (or ground? (every? v/ground-var? infields-set)))))

        ;; Adds operations to tail until can't anymore. Returns new tail
        (fixed-point-operations [tail]
          (if-let [op (s/find-first (partial op-allowed? tail)
                                    (:operations tail))]
            (recur (add-op tail op))
            tail))

        (add-drift-op [tail equality-sets rename-map new-drift-map]
          (let [eq-assemblies (map w/equal equality-sets)
                outfields (vec (keys rename-map))
                rename-in (vec (vals rename-map))
                rename-assembly (if (seq rename-in)
                                  (w/identity rename-in :fn> outfields :> Fields/SWAP)
                                  identity)
                assembly   (apply w/compose-straight-assemblies
                                  (concat eq-assemblies [rename-assembly]))
                infields (vec (apply concat rename-in equality-sets))
                tail (connect-op tail (p/predicate p/operation
                                                   assembly
                                                   infields
                                                   outfields
                                                   false))
                newout (difference (set (:available-fields tail))
                                   (set rename-in))]
            (merge tail {:drift-map new-drift-map
                         :available-fields newout} )))

        (drift-equalities [drift-m available-fields]
          (let [eqmap (select-keys (u/reverse-map
                                    (select-keys drift-m available-fields))
                                   available-fields)
                equality-sets (map (fn [[k v]] (conj v k)) eqmap)]
            ))

        (determine-drift
          [drift-map available-fields]
          ;; This function kicks out a new drift map, a set of fields
          ;; that are meant to be equal, and a map of items to rename.
          (let [available-set (set available-fields)
                rename-map (reduce (fn [m f]
                                     (let [drift (drift-map f)]
                                       (if (and drift (not (available-set drift)))
                                         (assoc m drift f)
                                         m)))
                                   {} available-fields)
                eqmap (select-keys (u/reverse-map
                                    (select-keys drift-map available-fields))
                                   available-fields)
                equality-sets (map (fn [[k v]] (conj v k)) eqmap)
                new-drift-map (->> equality-sets
                                   (apply concat (vals rename-map))
                                   (apply dissoc drift-map))]
            (prn "NEW DRIFT: " new-drift-map)
            [new-drift-map equality-sets rename-map]))]

  (defn- add-ops-fixed-point
    "Adds operations to tail until can't anymore. Returns new tail"
    [tail]
    (let [{:keys [drift-map available-fields] :as tail} (fixed-point-operations tail)
          [new-drift-map equality-sets rename-map]
          (determine-drift drift-map available-fields)]
      (if (and (empty? equality-sets)
               (empty? rename-map))
        tail
        (recur (add-drift-op tail equality-sets rename-map new-drift-map))))))

;; ## Join Field Detection

(defn- tail-fields-intersection [& tails]
  (->> tails
       (map #(set (:available-fields %)))
       (apply intersection)))

(defn- joinable? [joinfields tail]
  (let [join-set (set joinfields)
        tailfields (set (:available-fields tail))]
    (and (subset? join-set tailfields)
         (or (:ground? tail)
             (every? v/unground-var? (difference tailfields join-set))))))

(defn- find-join-fields [tail1 tail2]
  (let [join-set (tail-fields-intersection tail1 tail2)]
    (when (every? (partial joinable? join-set) [tail1 tail2])
      join-set)))

(defn- select-join
  "Splits tails into [join-fields {join set} {rest of tails}] This is
   unoptimal. It's better to rewrite this as a search problem to find
   optimal joins"
  [tails]
  (let [max-join (->> (u/all-pairs tails)
                      (map (fn [[t1 t2]]
                             (or (find-join-fields t1 t2) [])))
                      (sort-by count)
                      (last))]
    (if (empty? max-join)
      (throw-illegal "Unable to join predicates together")
      (cons (vec max-join)
            (s/separate (partial joinable? max-join) tails)))))

(defn- intersect-drift-maps
  [drift-maps]
  (let [tokeep (->> drift-maps
                    (map #(set (seq %)))
                    (apply intersection))]
    (u/pairs->map (seq tokeep))))

(defn- select-selector [seq1 selector]
  (mapcat (fn [o b] (if b [o])) seq1 selector))

;; ## Tail Merging

(defn- merge-tails
  "The first call begins with an empty graph and a bunch of generator
  tails, each with a list of operations that could be applied. Based
  on the op-allowed logic, these tails try to consume as many
  operations as possible before giving up at a fixed point."
  [graph tails]
  ;; Start by adding as many operations as possible to each tail.
  (let [tails (map add-ops-fixed-point tails)]
    (if (= 1 (count tails))
      ;; once we get down to a single generator -- either because we
      ;; only had one to begin with, or because a join gets us back to
      ;; basics -- go ahead and mark the tail as ground? and apply the
      ;; rest of the operations.
      (add-ops-fixed-point (merge (first tails) {:ground? true}))

      ;; Otherwise, we need to go into join mode.
      (let [[join-fields join-set rest-tails] (select-join tails)
            join-node             (g/create-node graph (p/predicate join join-fields))
            join-set-vars    (map find-generator-join-set-vars (map :node join-set))
            available-fields (vec (set (apply concat
                                              (cons (apply concat join-set-vars)
                                                    (select-selector
                                                     (map :available-fields join-set)
                                                     (map not join-set-vars))))))
            new-ops (vec (apply intersection (map #(set (:operations %)) join-set)))
            new-drift-map    (intersect-drift-maps (map :drift-map join-set))]
        (debug-print "Selected join" join-fields join-set)
        (debug-print "Join-set-vars" join-set-vars)
        (debug-print "Available fields" available-fields)
        (dorun (map #(g/create-edge (:node %) join-node) join-set))
        (recur graph (cons (struct tailstruct
                                   (s/some? :ground? join-set)
                                   new-ops
                                   new-drift-map
                                   available-fields
                                   join-node)
                           rest-tails))))))

;; ## Aggregation Operations
;;
;; The following operations deal with Cascalog's aggregations. I think
;; we can replace all of this by delegating out to the GroupBy
;; implementation in operations.

(letfn [
        ;; Returns the union of all grouping fields and all outputs
        ;; for every aggregation field. These are the only fields
        ;; available after the aggregation.
        (agg-available-fields
          [grouping-fields aggs]
          (->> aggs
               (map #(set (:outfields %)))
               (apply union)
               (union (set grouping-fields))
               (vec)))

        ;; These are the operations that go into the
        ;; aggregators.
        (agg-infields [sort-fields aggs]
          (->> aggs
               (map #(set (:infields %)))
               (apply union (set sort-fields))
               (vec)))

        ;; Returns [new-grouping-fields inserter-assembly]. If no
        ;; grouping fields are supplied, ths function groups on 1,
        ;; which forces a global grouping.
        (normalize-grouping
          [grouping-fields]
          (if (seq grouping-fields)
            [grouping-fields identity]
            (let [newvar (v/gen-nullable-var)]
              [[newvar] (w/insert newvar 1)])))

        (mk-combined-aggregator [pagg argfields outfields]
          (w/raw-every (w/fields argfields)
                       (ClojureCombinedAggregator. (w/fields outfields) pagg)
                       Fields/ALL))

        (mk-agg-arg-fields [fields]
          (when (seq fields)
            (w/fields fields)))

        (mk-parallel-aggregator [grouping-fields aggs]
          (let [argfields  (map #(mk-agg-arg-fields (:infields %)) aggs)
                tempfields (map #(v/gen-nullable-vars (count (:outfields %))) aggs)
                specs      (map :parallel-agg aggs)
                combiner (ClojureCombiner. (w/fields grouping-fields)
                                           argfields
                                           (w/fields (apply concat tempfields))
                                           specs)]
            [[(w/raw-each Fields/ALL combiner Fields/RESULTS)]
             (map mk-combined-aggregator specs tempfields (map :outfields aggs))]))

        (mk-parallel-buffer-agg [grouping-fields agg]
          [[((:parallel-agg agg) grouping-fields)]
           [(:serial-agg-assembly agg)]])

        ;; returns [pregroup vec, postgroup vec]
        (build-agg-assemblies
          [grouping-fields aggs]
          (cond (and (= 1 (count aggs))
                     (:parallel-agg (first aggs))
                     (:buffer? (first aggs)))
                (mk-parallel-buffer-agg grouping-fields
                                        (first aggs))

                (every? :parallel-agg aggs)
                (mk-parallel-aggregator grouping-fields aggs)

                :else [[identity] (map :serial-agg-assembly aggs)]))

        ;; Create a groupby operation, respecting grouping and sorting
        ;; options.

        (mk-group-by
          [grouping-fields options]
          (let [{s :sort rev :reverse} options]
            (if (seq s)
              (w/group-by grouping-fields s rev)
              (w/group-by grouping-fields))))]

  ;; Note that the final group that pops out of this thing is keeping
  ;; track of the input fields to each of the aggregators as well as
  ;; the total number of available output fields. These include the
  ;; fields generated by the aggregations, as well as the fields used
  ;; for grouping.
  (defn- build-agg-tail [options prev-tail grouping-fields aggs]
    (debug-print "Adding aggregators to tail" options prev-tail grouping-fields aggs)
    (when (and (empty? aggs) (:sort options))
      (throw-illegal "Cannot specify a sort when there are no aggregators"))
    (if (and (not (:distinct options)) (empty? aggs))
      prev-tail
      (let [aggs (or (not-empty aggs) [p/distinct-aggregator])
            [grouping-fields inserter] (normalize-grouping grouping-fields)
            [prep-aggs postgroup-aggs] (build-agg-assemblies grouping-fields aggs)
            assem (apply w/compose-straight-assemblies
                         (concat [inserter]
                                 (map :pregroup-assembly aggs)
                                 prep-aggs
                                 [(mk-group-by grouping-fields options)]
                                 postgroup-aggs
                                 (map :post-assembly aggs)))
            total-fields (agg-available-fields grouping-fields aggs)
            all-agg-infields  (agg-infields (:sort options) aggs)
            prev-node (:node prev-tail)
            node (g/create-node (g/get-graph prev-node)
                                (p/predicate group assem
                                             all-agg-infields
                                             total-fields))]
        (g/create-edge prev-node node)
        (struct tailstruct
                (:ground? prev-tail)
                (:operations prev-tail)
                (:drift-map prev-tail)
                total-fields
                node)))))

(defn projection-fields [needed-vars allfields]
  (let [needed-set (set needed-vars)
        all-set    (set allfields)
        inter      (intersection needed-set all-set)]
    (cond
     ;; maintain ordering when =, this is for output of generators to
     ;; match declared ordering
     (= inter needed-set) needed-vars

     ;; this happens during global aggregation, need to keep one field
     ;; in
     (empty? inter) [(first allfields)]
     :else (vec inter))))

(defmulti node->generator (fn [pred & _] (:type pred)))

(defmethod node->generator :generator [pred prevgens]
  (when (not-empty prevgens)
    (throw (RuntimeException. "Planner exception: Generator has inbound nodes")))
  pred)

(defn join-fields-selector [num-fields]
  (cascalog.fluent.def/mapfn [& args]
    (let [joins (partition num-fields args)]
      (or (s/find-first (partial s/some? (complement nil?)) joins)
          (repeat num-fields nil)))))

(w/defmapop truthy? [arg]
  (if arg true false))

(defn- replace-join-fields [join-fields join-renames fields]
  (let [replace-map (zipmap join-fields join-renames)]
    (reduce (fn [ret f]
              (let [newf (replace-map f)
                    newf (if newf newf f)]
                (conj ret newf)))
            [] fields)))

(defn- generate-join-fields [numfields numpipes]
  (take numpipes (repeatedly (partial v/gen-nullable-vars numfields))))

(defn- new-pipe-name [prevgens]
  (.getName (:pipe (first prevgens))))

(defn- gen-join-type [gen]
  (cond (:join-set-var gen) :outerone
        (:ground? gen)      :inner
        :else               :outer))

;; split into inner-gens outer-gens join-set-gens
(defmethod node->generator :join [pred prevgens]
  (debug-print "Creating join" pred)
  (debug-print "Joining" prevgens)
  (let [join-fields (:infields pred)
        num-join-fields (count join-fields)
        sourcemap   (apply merge (map :sourcemap prevgens))
        trapmap     (apply merge (map :trapmap prevgens))
        {inner-gens :inner
         outer-gens :outer
         outerone-gens :outerone} (group-by gen-join-type prevgens)
        join-set-fields (map :join-set-var outerone-gens)
        prevgens    (concat inner-gens outer-gens outerone-gens) ; put them in order
        infields    (map :outfields prevgens)
        inpipes     (map (fn [p f] (w/assemble p (w/select f) (w/pipe-rename (u/uuid))))
                         (map :pipe prevgens)
                         infields)      ; is this necessary?
        join-renames (generate-join-fields num-join-fields (count prevgens))
        rename-fields (flatten (map (partial replace-join-fields join-fields) join-renames infields))
        keep-fields (vec (set (apply concat
                                     (cons join-set-fields
                                           (map :outfields
                                                (concat inner-gens outer-gens))))))
        cascalogjoin (concat (repeat (count inner-gens) CascalogJoiner$JoinType/INNER)
                             (repeat (count outer-gens) CascalogJoiner$JoinType/OUTER)
                             (repeat (count outerone-gens) CascalogJoiner$JoinType/EXISTS))
        joined      (apply w/assemble inpipes
                           (concat
                            [(w/co-group (repeat (count inpipes) join-fields)
                                         rename-fields (CascalogJoiner. cascalogjoin))]
                            (mapcat
                             (fn [gen joinfields]
                               (if-let [join-set-var (:join-set-var gen)]
                                 [(truthy? (take 1 joinfields) :fn> [join-set-var] :> Fields/ALL)]))
                             prevgens join-renames)
                            [(join-fields-selector [num-join-fields] (flatten join-renames) :fn> join-fields :> Fields/SWAP)
                             (w/select keep-fields)
                             ;; maintain the pipe name (important for setting traps on subqueries)
                             (w/pipe-rename (new-pipe-name prevgens))]))]
    (p/predicate p/generator nil true sourcemap joined keep-fields trapmap)))

(defmethod node->generator :operation [pred prevgens]
  (when-not (= 1 (count prevgens))
    (throw (RuntimeException. "Planner exception: operation has multiple inbound generators")))
  (let [prevpred (first prevgens)]
    (merge prevpred {:outfields (concat (:outfields pred) (:outfields prevpred))
                     :pipe ((:assembly pred) (:pipe prevpred))})))

;; Look at the previous generators -- we should only have a single
;; incoming generator in this case, since we're pushing into a single
;; grouping. The output fields that are remaining need to follow this
;; damned logic.
;;
;; Basically, Nathan is implementing this ad-hoc field algebra...
(defmethod node->generator :group [pred prevgens]
  (when-not (= 1 (count prevgens))
    (throw (RuntimeException. "Planner exception: group has multiple inbound generators")))
  (let [prevpred (first prevgens)]
    (merge prevpred {:outfields (:totaloutfields pred)
                     :pipe ((:assembly pred) (:pipe prevpred))})))

;; forceproject necessary b/c *must* reorder last set of fields coming
;; out to match declared ordering

;; So, it looks like this function builds up a generator of tuples,
;; which in Nathan's world is a pipe, some output fields and the
;; remaining information for the FlowDef.
;;
;; Other than the Fields algebra, I think I've got it.

(comment
  "This is what it comes down to. We want to convert nodes to
  generators that can be compiled down into the FlowDef
  representation. This'll be accomplished by keeping around a graph
  with a bunch of nodes, and resolving the input generators down."

  "Nodes are generator, join, operation, group."
  (defpredicate generator
    :join-set-var
    :ground?
    :sourcemap
    :pipe
    :outfields
    :trapmap))

;; This projection assembly checks every output field against the
;; existing "allfields" deal. If the fields match up and no project is
;; being forced, then the function goes ahead and passes identity back
;; out.
;;
;; The final compilation step (the subquery step) requires a forced
;; projection, since a subquery declares its output variables.

(letfn [(mk-projection-assembly
          [forceproject projection-fields allfields]
          (if (and (not forceproject)
                   (= (set projection-fields)
                      (set allfields)))
            identity
            (w/select projection-fields)))]

  (defn build-generator [forceproject needed-vars node]
    (let [
          ;; Get the current operation at this node.
          pred           (g/get-value node)

          ;; The input had some required variables -- I've got to get
          ;; my required variables and concatenate them onto the set
          ;; of required variables.
          my-needed (vec (set (concat (:infields pred) needed-vars)))

          ;; Now, go ahead and build up generators for every input
          ;; node in the graph. This will recursively walk backward
          ;; along the graph until it finds the root generators, I
          ;; think.
          ;;
          ;; This is the stage where we can introduce the projection
          ;; pushdown. Currently, this has a side effect, but I think
          ;; we can get rid of that.
          prev-gens (doall (map (partial build-generator false my-needed)
                                (g/get-inbound-nodes node)))
          newgen (node->generator pred prev-gens)
          project-fields (projection-fields needed-vars (:outfields newgen)) ]
      (debug-print "build gen:" my-needed project-fields pred)
      (if (and forceproject (not= project-fields needed-vars))
        (throw-runtime (format "Only able to build to %s but need %s. Missing %s"
                               project-fields
                               needed-vars
                               (vec (difference (set needed-vars)
                                                (set project-fields)))))
        (merge newgen
               {:pipe ((mk-projection-assembly forceproject
                                               project-fields
                                               (:outfields newgen))
                       (:pipe newgen))
                :outfields project-fields})))))

;; ## Query Building

(defn- split-predicates
  "returns [generators operations aggregators]."
  [predicates]
  (let [{ops :operation aggs :aggregator gens :generator}
        (merge {:operation [] :aggregator [] :generator []}
               (group-by :type predicates))]
    (when (and (> (count aggs) 1)
               (some :buffer? aggs))
      (throw-illegal "Cannot use both aggregators and buffers in same grouping"))
    [gens ops aggs]))

(defn- build-query
  "This is the big mother. This is where the query gets built and
  where I'll be spending some good time."
  [out-vars raw-predicates]
  (debug-print "outvars:" out-vars)
  (debug-print "raw predicates:" raw-predicates)
  (let [
        ;; Parse the options out of the predicates,
        [raw-opts raw-predicates] (s/separate (comp keyword? first) raw-predicates)

        ;; and assemble the option map.
        option-m (opts/generate-option-map raw-opts)

        ;; rewrite all predicates
        [raw-predicates drift-map] (parse/parse-predicates raw-predicates)

        ;; Split up generators, operations and aggregators.
        [gens ops aggs] (->> raw-predicates
                             (map (partial apply p/build-predicate option-m))
                             (split-predicates))

        rule-graph (g/mk-graph)
        joined (->> gens
                    (map (fn [{:keys [ground? outfields] :as g}]
                           ;; Every generator is mapped to a tail
                           ;; struct with a list of all aggregations
                           ;; that could possibly be applied. The
                           ;; graph starts with a bunch of nodes
                           ;; representing the generators (and not
                           ;; connected to anything).
                           (struct tailstruct
                                   ground?
                                   ops
                                   drift-map
                                   outfields
                                   (g/create-node rule-graph g))))
                    (merge-tails rule-graph))
        ;; Group by the pre-aggregation variables that are in the
        ;; result vector.
        grouping-fields (seq (intersection (set (:available-fields joined))
                                           (set out-vars)))
        agg-tail (build-agg-tail option-m joined grouping-fields aggs)
        {:keys [operations node]} (add-ops-fixed-point agg-tail)]
    (if (not-empty operations)
      (throw-runtime "Could not apply all operations: " (pr-str operations))
      (build-generator true out-vars node))))

;; ## Query Compilation
;;
;; This is the entry point from "construct".
;;
;; Before compilation, all predicates are normalized down to clojure
;; predicates.
;;
;; Query compilation steps are as follows:
;;
;; 1. Normalize all predicates
;; 2. Expand predicate macros
;; 3. Desugar all of the argument selectors (remember positional!)

(defn query-signature?
  "Accepts the normalized return vector of a Cascalog form and returns
  true if the return vector is from a subquery, false otherwise. (A
  predicate macro would trigger false, for example.)"
  [vars]
  (not (some v/cascalog-keyword? vars)))

;; This is the main entry point from api.clj. What's the significance
;; of a query vs a predicate macro? The main thing in the current API
;; is that a query triggers an actual grouping, and compiles down to
;; another generator with a pipe, etc, while a predicate macro is just
;; a collection of predicates that expands out within another
;; subquery.

(defn build-rule
  "Construct a query or predicate macro functionally. When
   constructing queries this way, operations should either be vars for
   operations or values defined using one of Cascalog's def
   macros. Vars must be stringified when passed to construct. If
   you're using destructuring in a predicate macro, the & symbol must
   be stringified as well.

  This is the entry point into the rules of the system. output
  variables and raw predicates come in, predicate macros are expanded,
  and the query (or another predicate macro) is compiled."
  [out-vars raw-predicates]
  (let [out-vars   (v/sanitize out-vars)
        predicates (->> raw-predicates
                        (map parse/to-predicate)
                        predmacro/expand-predicate-macros)]
    (if (query-signature? out-vars)
      (build-query out-vars predicates)
      (let [parsed (parse/parse-variables out-vars :<)]
        (predmacro/build-predicate-macro (parsed :<<)
                                         (parsed :>>)
                                         predicates)))))
