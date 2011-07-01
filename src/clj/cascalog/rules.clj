 ;    Copyright 2010 Nathan Marz
 ; 
 ;    This program is free software: you can redistribute it and/or modify
 ;    it under the terms of the GNU General Public License as published by
 ;    the Free Software Foundation, either version 3 of the License, or
 ;    (at your option) any later version.
 ; 
 ;    This program is distributed in the hope that it will be useful,
 ;    but WITHOUT ANY WARRANTY; without even the implied warranty of
 ;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 ;    GNU General Public License for more details.
 ; 
 ;    You should have received a copy of the GNU General Public License
 ;    along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns cascalog.rules
  (:use [cascalog vars util graph debug])
  (:use clojure.contrib.set)
  (:use [clojure.set :only [intersection union difference]])
  (:use [clojure.contrib.set :only [subset?]])
  (:use [clojure.contrib.seq-utils :only [find-first separate]])
  (:use [clojure.walk :only [postwalk]])
  (:require [cascalog [workflow :as w] [predicate :as p] [hadoop :as hadoop]])
  (:import [cascading.tap Tap])
  (:import [cascading.tuple Fields Tuple TupleEntry])
  (:import [cascading.flow Flow FlowConnector])
  (:import [cascading.pipe Pipe])
  (:import [cascading.pipe.cogroup CascalogJoiner CascalogJoiner$JoinType])
  (:import [cascalog CombinerSpec ClojureCombiner ClojureCombinedAggregator Util])
  (:import [org.apache.hadoop.mapred JobConf])
  (:import [java.util ArrayList]))


;; source can be a cascalog-tap, subquery, or cascading tap
;; sink can be a cascading tap, a sink function, or a cascalog-tap
(defstruct cascalog-tap :type :source :sink)

;; infields for a join are the names of the join fields
(p/defpredicate join :infields)
(p/defpredicate group :assembly :infields :totaloutfields)

(defn- find-generator-join-set-vars [node]
  (let [pred (get-value node)
        inbound-nodes (get-inbound-nodes node)]
    (cond
      (#{:join :group} (:type pred))
      nil

      (= :generator (:type pred))
      (if-let [v (:join-set-var pred)] [v])

      true
      (do
        (when-not (= 1 (count inbound-nodes))
          (throw
            (RuntimeException.
              "Planner exception: Unexpected number of inbound nodes to non-generator predicate")))
        (recur (first inbound-nodes))
        ))))

(defn- split-predicates
  "returns [generators operations aggregators]."
  [predicates]
  (let [{ops :operation
         aggs :aggregator
         gens :generator} (merge {:operation [] :aggregator [] :generator []}
                                (group-by :type predicates))]
    (when (and (> (count aggs) 1) (some :buffer? aggs))
      (throw (IllegalArgumentException. "Cannot use both aggregators and buffers in same grouping")))
    [gens ops aggs] ))

(defstruct tailstruct :ground? :operations :drift-map :available-fields :node)

(defn- connect-op [tail op]
  (let [new-node (connect-value (:node tail) op)
        new-outfields (concat (:available-fields tail) (:outfields op))]
        (struct tailstruct (:ground? tail) (:operations tail) (:drift-map tail) new-outfields new-node)))

(defn- add-op [tail op]
  (debug-print "Adding op to tail " op tail)
  (let [tail (connect-op tail op)
        new-ops (remove-first (partial = op) (:operations tail))]
        (merge tail {:operations new-ops})))


(defn- op-allowed? [tail op]
  (let [ground? (:ground? tail)
        available-fields (:available-fields tail)
        join-set-vars (find-generator-join-set-vars (:node tail))
        infields-set (set (:infields op))]
    (and (or (:allow-on-genfilter? op) (empty? join-set-vars))
         (subset? infields-set (set available-fields))
         (or ground? (every? ground-var? infields-set)))
    ))

(defn- fixed-point-operations
  "Adds operations to tail until can't anymore. Returns new tail"
  [tail]
    (if-let [op (find-first (partial op-allowed? tail) (:operations tail))]
      (recur (add-op tail op))
      tail ))

;; TODO: refactor and simplify drift algorithm
(defn- add-drift-op [tail equality-sets rename-map new-drift-map]
  (let [eq-assemblies (map w/equal equality-sets)
        outfields (vec (keys rename-map))
        rename-in (vec (vals rename-map))
        rename-assembly (if-not (empty? rename-in) (w/identity rename-in :fn> outfields :> Fields/SWAP) identity)
        assembly   (apply w/compose-straight-assemblies (concat eq-assemblies [rename-assembly]))
        infields (vec (apply concat rename-in equality-sets))
        tail (connect-op tail (p/predicate p/operation assembly infields outfields false))
        newout (difference (set (:available-fields tail)) (set rename-in))]
        (merge tail {:drift-map new-drift-map :available-fields newout} )))

(defn- determine-drift [drift-map available-fields]
  (let [available-set (set available-fields)
        rename-map  (reduce (fn [m f]
                      (let [drift (drift-map f)]
                        (if (and drift (not (available-set drift)))
                          (assoc m drift f)
                          m )))
                      {} available-fields)
        eqmap      (select-keys (reverse-map (select-keys drift-map available-fields)) available-fields)
        equality-sets (map (fn [[k v]] (conj v k)) eqmap)
        new-drift-map (apply dissoc drift-map (apply concat (vals rename-map) equality-sets))]
    [new-drift-map equality-sets rename-map] ))

(defn- add-ops-fixed-point
  "Adds operations to tail until can't anymore. Returns new tail"
  [tail]
  (let [tail (fixed-point-operations tail)
        drift-map (:drift-map tail)
        [new-drift-map equality-sets rename-map] (determine-drift drift-map (:available-fields tail))]
    (if (and (empty? equality-sets) (empty? rename-map))
      tail
      (recur (add-drift-op tail equality-sets rename-map new-drift-map)))
    ))

(defn- tail-fields-intersection [& tails]
  (intersection (map #(set (:available-fields %)) tails)))

(defn- joinable? [joinfields tail]
  (let [join-set (set joinfields)
        tailfields (set (:available-fields tail))]
    (and (subset? join-set tailfields)
        (or (:ground? tail)
            (every? unground-var? (difference tailfields join-set))
        ))))

(defn- find-join-fields [tail1 tail2]
  (let [set1 (set (:available-fields tail1))
        set2 (set (:available-fields tail2))
        join-set (intersection set1 set2)]
  (if (every? (partial joinable? join-set) [tail1 tail2])
      join-set
      [] )))

(defn- select-join
  "Splits tails into [join-fields {join set} {rest of tails}]
   This is unoptimal. It's better to rewrite this as a search problem to find optimal joins"
  [tails]
  (let [pairs     (all-pairs tails)
        sections  (map (fn [[t1 t2]]
                          (find-join-fields t1 t2))
                      pairs)
        max-join  (last (sort-by count sections))]
      (when (empty? max-join) (throw (IllegalArgumentException. "Unable to join predicates together")))
      (cons (vec max-join) (separate (partial joinable? max-join) tails))
    ))

(defn- intersect-drift-maps [drift-maps]
  (let [drift-sets (map #(set (seq %)) drift-maps)
        tokeep     (apply intersection drift-sets)]
    (pairs2map (seq tokeep))))

(defn- select-selector [seq1 selector]
  (mapcat (fn [o b] (if b [o])) seq1 selector))

(defn- merge-tails [graph tails]
  (let [tails (map add-ops-fixed-point tails)]
    (if (= 1 (count tails))
      (add-ops-fixed-point (merge (first tails) {:ground? true}))  ; if still unground, allow operations to be applied
      (let [[join-fields join-set rest-tails] (select-join tails)
            join-node             (create-node graph (p/predicate join join-fields))
            join-set-vars       (map find-generator-join-set-vars (map :node join-set))
            available-fields    (vec (set (apply concat
                                    (cons (apply concat join-set-vars)
                                      (select-selector
                                        (map :available-fields join-set)
                                        (map not join-set-vars))
                                      ))))
            new-ops               (vec (apply intersection (map #(set (:operations %)) join-set)))
            new-drift-map         (intersect-drift-maps (map :drift-map join-set))]
        (debug-print "Selected join" join-fields join-set)
        (debug-print "Join-set-vars" join-set-vars)
        (debug-print "Available fields" available-fields)
        (dorun (map #(create-edge (:node %) join-node) join-set))
        (recur graph (cons (struct tailstruct (some? :ground? join-set) new-ops new-drift-map
                              available-fields join-node) rest-tails))
        ))))

(defn- agg-available-fields [grouping-fields aggs]
  (vec (union (set grouping-fields) (apply union (map #(set (:outfields %)) aggs)))))

(defn- agg-infields [sort-fields aggs]
  (vec (apply union (set sort-fields) (map #(set (:infields %)) aggs))))

(defn- normalize-grouping
  "Returns [new-grouping-fields inserter-assembly]"
  [grouping-fields]
  (if-not (empty? grouping-fields)
          [grouping-fields identity]
          (let [newvar (gen-nullable-var)]
            [[newvar] (w/insert newvar 1)]
            )))

(defn- specify-parallel-agg [agg]
  (let [pagg (:parallel-agg agg)]
    (CombinerSpec. (w/fn-spec (:init-var pagg)) (w/fn-spec (:combine-var pagg)))))

(defn- mk-combined-aggregator [combiner-spec argfields outfields]
  (w/raw-every (w/fields argfields) (ClojureCombinedAggregator. (w/fields outfields) (. combiner-spec combiner_spec)) Fields/ALL))

(defn mk-agg-arg-fields [fields]
  (if (empty? fields) nil (w/fields fields)))

(defn- mk-parallel-aggregator [grouping-fields aggs]
  (let [argfields (map #(mk-agg-arg-fields (:infields %)) aggs)
        tempfields (map #(gen-nullable-vars (count (:outfields %))) aggs)
        specs (map specify-parallel-agg aggs)
        combiner (ClojureCombiner. (w/fields grouping-fields)
                                   argfields
                                   (w/fields (apply concat tempfields))
                                   specs)]
        [[(w/raw-each Fields/ALL combiner Fields/RESULTS)]
         (map mk-combined-aggregator specs tempfields (map :outfields aggs))] ))

(defn- mk-parallel-buffer-agg [grouping-fields agg]
  [[((:parallel-agg agg) grouping-fields)] [(:serial-agg-assembly agg)]] )

(defn- build-agg-assemblies
  "returns [pregroup vec, postgroup vec]"
  [grouping-fields aggs]
  (cond (and (= 1 (count aggs))
             (:parallel-agg (first aggs))
             (:buffer? (first aggs)))       (mk-parallel-buffer-agg grouping-fields (first aggs))
        (every? :parallel-agg aggs)         (mk-parallel-aggregator grouping-fields aggs)
        true                                [[identity] (map :serial-agg-assembly aggs)]))

(defn- mk-group-by [grouping-fields options]
  (let [s (:sort options)]
   (if-not (empty? s)
      (w/group-by grouping-fields (:sort options) (:reverse options))
      (w/group-by grouping-fields))))

(defn- build-agg-tail [options prev-tail grouping-fields aggs]
  (debug-print "Adding aggregators to tail" options prev-tail grouping-fields aggs)
  (when (and (empty? aggs) (:sort options))
    (throw (IllegalArgumentException. "Cannot specify a sort when there are no aggregators" )))
  (if (and (not (:distinct options)) (empty? aggs))
    prev-tail
    (let [aggs              (if-not (empty? aggs) aggs [p/distinct-aggregator])
          [grouping-fields
           inserter]        (normalize-grouping grouping-fields)
          prev-node    (:node prev-tail)
          [prep-aggs postgroup-aggs] (build-agg-assemblies grouping-fields aggs)
          assem        (apply w/compose-straight-assemblies (concat
                         [inserter]
                         (map :pregroup-assembly aggs)
                         prep-aggs
                         [(mk-group-by grouping-fields options)]
                         postgroup-aggs
                         (map :post-assembly aggs)
                       ))
        total-fields (agg-available-fields grouping-fields aggs)
        all-agg-infields  (agg-infields (:sort options) aggs)
        node         (create-node (get-graph prev-node)
                        (p/predicate group assem all-agg-infields total-fields))]
     (create-edge prev-node node)
     (struct tailstruct (:ground? prev-tail) (:operations prev-tail) (:drift-map prev-tail) total-fields node))
    ))

(defn projection-fields [needed-vars allfields]
  (let [needed-set (set needed-vars)
        all-set    (set allfields)
        inter      (intersection needed-set all-set)]
        (cond (= inter needed-set) needed-vars  ; maintain ordering when =, this is for output of generators to match declared ordering
              (empty? inter) [(first allfields)]  ; this happens during global aggregation, need to keep one field in
              true (vec inter)
        )))

(defn- mk-projection-assembly [forceproject projection-fields allfields]
    (if (and (not forceproject) (= (set projection-fields) (set allfields)))
      identity
      (w/select projection-fields)))

(defmulti node->generator (fn [pred & rest] (:type pred)))

(defmethod node->generator :generator [pred prevgens]
  (when (not-empty prevgens)
    (throw (RuntimeException. "Planner exception: Generator has inbound nodes")))
  pred )

(w/defmapop [join-fields-selector [num-fields]] [& args]
  (let [joins (partition num-fields args)]
    (if-ret (find-first (partial some? (complement nil?)) joins)
      (repeat num-fields nil))))

(w/defmapop truthy [arg]
  (if arg true false))

(defn- replace-join-fields [join-fields join-renames fields]
  (let [replace-map (zipmap join-fields join-renames)]
    (reduce (fn [ret f]
      (let [newf (replace-map f)
            newf (if newf newf f)]
            (conj ret newf)))
      [] fields)))

(defn- generate-join-fields [numfields numpipes]
  (take numpipes (repeatedly (partial gen-nullable-vars numfields))))

(defn- new-pipe-name [prevgens]
  (.getName (:pipe (first prevgens))))

(defn- gen-join-type [gen]
  (cond (:join-set-var gen) :outerone
        (:ground? gen)      :inner
        true                :outer
    ))

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
        prevgens    (concat inner-gens outer-gens outerone-gens)  ; put them in order
        infields    (map :outfields prevgens)
        inpipes     (map (fn [p f] (w/assemble p (w/select f) (w/pipe-rename (uuid)))) (map :pipe prevgens) infields)  ; is this necessary?
        join-renames (generate-join-fields num-join-fields (count prevgens))
        rename-fields (flatten (map (partial replace-join-fields join-fields) join-renames infields))
        keep-fields (vec (set (apply concat
                      (cons join-set-fields
                            (map :outfields
                              (concat inner-gens outer-gens))))))
        cascalogjoin (concat (repeat (count inner-gens) CascalogJoiner$JoinType/INNER)
                             (repeat (count outer-gens) CascalogJoiner$JoinType/OUTER)
                             (repeat (count outerone-gens) CascalogJoiner$JoinType/EXISTS)
                             )
        joined      (apply w/assemble inpipes
                      (concat
                        [(w/co-group (repeat (count inpipes) join-fields)
                          rename-fields (CascalogJoiner. cascalogjoin))]
                        (mapcat
                          (fn [gen joinfields]
                            (if-let [join-set-var (:join-set-var gen)]
                              [(truthy (take 1 joinfields) :fn> [join-set-var] :> Fields/ALL)]
                              ))
                          prevgens join-renames)
                        [(join-fields-selector [num-join-fields]
                          (flatten join-renames) :fn> join-fields :> Fields/SWAP)
                         (w/select keep-fields)
                        ;; maintain the pipe name (important for setting traps on subqueries)
                        (w/pipe-rename (new-pipe-name prevgens))]))]
        (p/predicate p/generator nil true sourcemap joined keep-fields trapmap)
    ))

(defmethod node->generator :operation [pred prevgens]
  (when-not (= 1 (count prevgens))
    (throw (RuntimeException. "Planner exception: operation has multiple inbound generators")))
  (let [prevpred (first prevgens)]
    (merge prevpred {:outfields (concat (:outfields pred) (:outfields prevpred))
            :pipe ((:assembly pred) (:pipe prevpred))})))

(defmethod node->generator :group [pred prevgens]
  (when-not (= 1 (count prevgens))
    (throw (RuntimeException. "Planner exception: group has multiple inbound generators")))
  (let [prevpred (first prevgens)]
    (merge prevpred {:outfields (:totaloutfields pred)
            :pipe ((:assembly pred) (:pipe prevpred))})))

;; forceproject necessary b/c *must* reorder last set of fields coming out to match declared ordering
(defn build-generator [forceproject needed-vars node]
  (let [pred           (get-value node)
        my-needed      (vec (set (concat (:infields pred) needed-vars)))
        prev-gens      (doall (map (partial build-generator false my-needed) (get-inbound-nodes node)))
        newgen         (node->generator pred prev-gens)
        project-fields (projection-fields needed-vars (:outfields newgen)) ]
        (debug-print "build gen:" my-needed project-fields pred)
        (when (and forceproject (not= project-fields needed-vars))
          (throw (RuntimeException. (str "Only able to build to " project-fields " but need " needed-vars))))
        (merge newgen {:pipe ((mk-projection-assembly forceproject project-fields (:outfields newgen)) (:pipe newgen))
                :outfields project-fields})))

(def DEFAULT-OPTIONS
  {:distinct true
   :sort nil
   :reverse false
   :trap nil})

(defn- validate-option-merge! [val-old val-new]
  (when (and (not (nil? val-old)) (not= val-old val-new))
    (throw (RuntimeException. "Same option set to conflicting values!")))
  val-new )

(defn- mk-options [opt-predicates]
  (merge DEFAULT-OPTIONS (apply merge-with validate-option-merge!
    (map (fn [p]
            (let [k (:key p)
                  v (:val p)
                  v (if (= :sort k) v (first v))
                  v (if (= :trap k) {:tap v :name (uuid)} v)]
            (if-not (contains? DEFAULT-OPTIONS k)
              (throw
                (IllegalArgumentException. (str k " is not a valid option predicate"))))
            {k v})) opt-predicates))))

(defn- mk-var-uniquer-reducer [out?]
  (fn [[preds vmap] [op opvar hof-args invars outvars]]
    (let [[updatevars vmap] (uniquify-vars (if out? outvars invars) out? vmap)
          [invars outvars] (if out? [invars updatevars] [updatevars outvars])]
      [(conj preds [op opvar hof-args invars outvars]) vmap]
      )))

(defn- uniquify-query-vars [out-vars raw-predicates]
  (let [[raw-predicates vmap] (reduce (mk-var-uniquer-reducer true) [[] {}] raw-predicates)
        [raw-predicates vmap] (reduce (mk-var-uniquer-reducer false) [[] vmap] raw-predicates)
        [out-vars vmap]       (uniquify-vars out-vars false vmap)
        drift-map             (mk-drift-map vmap)]
      [out-vars raw-predicates drift-map] ))

(defn split-outvar-constants [[op opvar hof-args invars outvars]]
  (let [[new-outvars newpreds] (reduce
                                 (fn [[outvars preds] v]
                                   (if (cascalog-var? v)
                                     [(conj outvars v) preds]
                                     (let [newvar (gen-nullable-var)]
                                       [(conj outvars newvar)
                                        (conj preds [(p/predicate p/outconstant-equal) nil nil [v newvar] []])]
                                       )))
                                 [[] []]
                                 outvars)]
  (cons [op opvar hof-args invars new-outvars] newpreds)
  ))

(defn- rewrite-predicate [[op opvar hof-args invars outvars :as predicate]]
  (if (and (p/generator? op) (not (empty? invars)))
    (do
      (when-not (= 1 (count outvars))
        (throw (IllegalArgumentException. (str "Generator filter can only have one outvar -> " outvars))))
      [(p/predicate p/generator-filter op (first outvars)) nil hof-args [] invars])
    predicate
    ))

(defn- parse-predicate [[op opvar vars]]
 (let [[vars hof-args] (if (p/hof-predicate? op)
                          [(rest vars) (collectify (first vars))]
                          [vars nil])
       {invars :<< outvars :>>} (p/parse-variables vars (p/predicate-default-var op))]
    [op opvar hof-args invars outvars] ))

(defn- build-query [out-vars raw-predicates]
  (debug-print "outvars:" out-vars)
  (debug-print "raw predicates:" raw-predicates)
  ;; TODO: split out a 'make predicates' function that does correct validation within it, ensuring unground vars appear only once
  (let [raw-predicates            (map parse-predicate raw-predicates)
        raw-predicates            (mapcat split-outvar-constants raw-predicates)
        raw-predicates            (map rewrite-predicate raw-predicates)
        ;; rewrite output constants again since input and output vars may have gotten shuffled
        raw-predicates            (mapcat split-outvar-constants raw-predicates)
        ;; TODO: this won't handle drift for generator filter outvars
        ;; should fix this by rewriting and simplifying how drift implementation works
        [out-vars raw-predicates
         drift-map]               (uniquify-query-vars out-vars raw-predicates)
        [raw-opts raw-predicates] (separate #(keyword? (first %)) raw-predicates)
        options                   (mk-options (map p/mk-option-predicate raw-opts))
        [gens ops aggs]           (split-predicates (map (partial apply p/build-predicate options) raw-predicates))
        rule-graph                (mk-graph)
        tails                     (map (fn [g] (struct tailstruct (:ground? g)
                                        ops drift-map (:outfields g) (create-node rule-graph g))) gens)
        joined                    (merge-tails rule-graph tails)
        grouping-fields           (seq (intersection (set (:available-fields joined)) (set out-vars)))
        agg-tail                  (build-agg-tail options joined grouping-fields aggs)
        tail                      (add-ops-fixed-point agg-tail)]
      (when (not-empty (:operations tail))
        (throw (RuntimeException. (str "Could not apply all operations " (pr-str (:operations tail))))))
      (build-generator true out-vars (:node tail))))

(defn- new-var-name! [replacements v]
  (let [new-name  (if (contains? @replacements v)
                    (@replacements v)
                    (uniquify-var v))]
    (swap! replacements assoc v new-name)
    new-name ))

(defn- pred-macro-updater [[replacements ret] [op opvar vars]]
  (let [newvars (postwalk #(if (cascalog-var? %)
                             (new-var-name! replacements %)
                             %)
                          vars)]
    [replacements (conj ret [op opvar newvars])] ))

(defn- build-predicate-macro-fn [invars-decl outvars-decl raw-predicates]
  (when-not (empty? (intersection (set invars-decl) (set outvars-decl)))
    (throw (RuntimeException. "Cannot declare the same var as an input and output to predicate macro")))
  (fn [invars outvars]
    (let [outvars (if (and (empty? outvars) (sequential? outvars-decl) (= 1 (count outvars-decl)))
                    [true]
                    outvars) ; kind of a hack, simulate using pred macros like filters
          replacements (atom (mk-destructured-seq-map invars-decl invars outvars-decl outvars))]
      (second (reduce pred-macro-updater [replacements []] raw-predicates))
      )))

(defn- build-predicate-macro [invars outvars raw-predicates]
  (p/predicate p/predicate-macro (build-predicate-macro-fn invars outvars raw-predicates)))

(defn- expand-predicate-macro [p vars]
  (let [{invars :<< outvars :>>} (p/parse-variables vars :<)]
    ((:pred-fn p) invars outvars)))

(defn- expand-predicate-macros [raw-predicates]
  (mapcat
    (fn [[p _ vars :as raw-predicate]]
      (if (p/predicate-macro? p)
        (expand-predicate-macros (expand-predicate-macro p vars))
        [raw-predicate] ))
    raw-predicates ))

(defn normalize-raw-predicates [raw-predicates]
  (for [[p v vars] raw-predicates]
    (if (var? p)
      [(var-get p) p vars] ; support passing around ops as vars
      [p v vars]
      )))

(defn build-rule [out-vars raw-predicates]
  (let [raw-predicates (expand-predicate-macros raw-predicates)
        raw-predicates (normalize-raw-predicates raw-predicates)
        parsed (p/parse-variables out-vars :?)]
    (if-not (empty? (parsed :?))
      (build-query out-vars raw-predicates)
      (build-predicate-macro (parsed :<<) (parsed :>>) raw-predicates)
      )))

(defn mk-raw-predicate [[op-sym & vars]]
  [op-sym (try-resolve op-sym) (vars2str vars)])

(def *JOB-CONF* {})

(defn connect-to-sink [gen sink]
  ((w/pipe-rename (uuid)) (:pipe gen)))

(defn generic-cascading-fields? [cfields]
  (or (.isSubstitution cfields)
      (.isUnknown cfields)
      (.isResults cfields)
      (.isSwap cfields)
      (.isReplace cfields)))

(defn generator-selector [gen & args]
  (if (instance? Tap gen)
    :tap
    (:type gen)
    ))

(defn normalize-sink-connection [sink subquery]
  (cond (fn? sink) (sink subquery)
        (map? sink) (normalize-sink-connection (:sink sink) subquery)
        true [sink subquery]))

(defn parse-exec-args [[f & rest :as args]]
  (if (instance? String f)
    [f rest]
    ["" args]))

(defn get-tuples [^Tap sink]
  (with-open [it (.openForRead sink (hadoop/job-conf *JOB-CONF*))]
    (doall
     (for [^TupleEntry t (iterator-seq it)]
       (vec (Util/coerceFromTuple (Tuple. (.getTuple t))))
       ))))

