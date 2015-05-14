(ns cascalog.logic.parse
  (:require [clojure.set :refer (difference intersection union subset?)]
            [clojure.zip :as czip]
            [jackknife.core :as u]
            [jackknife.seq :as s]
            [cascalog.logic.def :as d]
            [cascalog.logic.algebra :as algebra]
            [cascalog.logic.vars :as v]
            [cascalog.logic.zip :as zip]
            [cascalog.logic.predicate :as p]
            [cascalog.logic.predmacro :as pm]
            [cascalog.logic.platform :as platform]
            [cascalog.logic.options :as opts])
  (:import [cascalog.logic.predicate
            Operation FilterOperation Aggregator Generator
            GeneratorSet RawPredicate RawSubquery]
           [clojure.lang IPersistentVector]
           [jcascalog Subquery]))

;; ## Variable Parsing

;; TODO: Note that this is the spot where we'd go ahead and add new
;; selectors to Cascalog. For example, say we wanted the ability to
;; pour the results of a query into a vector directly; :>> ?a. This is
;; the place.

;; TODO: validation on the arg-m. We shouldn't ever have the sugar arg
;; and the non-sugar arg. Move the examples out to tests.

(defn desugar-selectors
  "Accepts a map of cascalog input or output symbol (:< or :>, for
  example) to var sequence, a <sugary input or output selector> and a
  <full vector input or output selector> and either destructures the
  non-sugary input or moves the sugary input into its proper
  place. For example:

 (desugar-selectors {:>> ([\"?a\"])} :> :>>)
 ;=> {:>> [\"?a\"]}

 (desugar-selectors {:> [\"?a\"] :<< [[\"?b\"]]} :> :>> :< :<<)
 ;=>  {:>> [\"?a\"], :<< [\"?b\"]}"
  [arg-m & sugar-full-pairs]
  (letfn [(desugar [m [sugar-k full-k]]
            (if-not (some m #{sugar-k full-k})
              m
              (-> m
                  (dissoc sugar-k)
                  (assoc full-k
                    (or (first (m full-k))
                        (m sugar-k))))))]
    (reduce desugar arg-m
            (partition 2 sugar-full-pairs))))

(defn expand-positional-selector
  "Accepts a map of cascalog selector to var sequence and, if the map
  contains an entry for Cascalog's positional selector, expands out
  the proper number of logic vars and replaces each entry specified
  within the positional map. This function returns the updated map."
  [arg-m]
  (if-let [[var-count selector-map] (:#> arg-m)]
    (let [expanded-vars (reduce (fn [v [pos var]]
                                  (assoc v pos var))
                                (vec (v/gen-nullable-vars var-count))
                                selector-map)]
      (-> arg-m
          (dissoc :#>)
          (assoc :>> expanded-vars)))
    arg-m))

(defn parse-variables
  "parses variables of the form ['?a' '?b' :> '!!c'] and returns a map
   of input variables, output variables, If there is no :>, defaults
   to selector-default."
  [vars default-selector]
  {:pre [(contains? #{:> :<} default-selector)]}
  (let [vars (cond (v/selector? (first vars)) vars
                   (some v/selector? vars) (cons :< vars)
                   :else (cons default-selector vars))
        {input :<< output :>>} (-> (s/to-map v/selector? vars)
                                   (desugar-selectors :> :>>
                                                      :< :<<)
                                   (expand-positional-selector))]
    {:input  (v/sanitize input)
     :output (v/sanitize output)}))

(defn default-selector
  "Default selector (either input or output) for this
  operation. Dispatches based on type."
  [op]
  (if (or (keyword? op)
          (p/filter? op))
    :< :>))

(extend-protocol p/IRawPredicate
  IPersistentVector
  (normalize [[op & rest]]
    (let [op (p/to-operation op)
          default (default-selector op)
          {:keys [input output]} (parse-variables rest (default-selector op))]
      (if (pm/predmacro? op)
        (mapcat p/normalize (pm/expand op input output))
        [(p/RawPredicate. op (not-empty input) (not-empty output))]))))

;; ## Unground Var Validation

(defn unground-outvars
  "For the supplied sequence of RawPredicate instances, returns a seq
  of all ungrounding vars in the output position."
  [predicates]
  (mapcat (comp (partial filter v/unground-var?) :output)
          predicates))

(defn unground-assertions!
  "Performs various validations on the supplied set of parsed
  predicates. If all validations pass, returns the sequence
  unchanged."
  [gens ops]
  (let [gen-outvars (unground-outvars gens)
        extra-vars  (difference (set (unground-outvars ops))
                                (set gen-outvars))
        dups (s/duplicates gen-outvars)]
    (when (not-empty extra-vars)
      (u/throw-illegal (str "Ungrounding vars must originate within a generator. "
                            extra-vars
                            " violate(s) the rules.")))
    (when (not-empty dups)
      (u/throw-illegal (str "Each ungrounding var can only appear once per query."
                            "The following are duplicated: "
                            dups)))))

(defn aggregation-assertions! [buffers aggs options]
  (when (and (not-empty aggs)
             (not-empty buffers))
    (u/throw-illegal "Cannot use both aggregators and buffers in same grouping"))
  ;; TODO: Move this into the fluent builder?
  (when (and (empty? aggs) (empty? buffers) (:sort options))
    (u/throw-illegal "Cannot specify a sort when there are no aggregators"))
  (when (> (count buffers) 1)
    (u/throw-illegal "Multiple buffers aren't allowed in the same subquery.")))

(defn validate-predicates! [preds opts]
  (let [grouped (group-by (fn [x]
                            (condp #(%1 %2) (:op x)
                              (partial platform/generator? platform/*platform*) :gens
                              d/bufferop? :buffers
                              d/aggregateop? :aggs
                              :ops))
                          preds)
        [options _] (opts/extract-options preds)]
    (unground-assertions! (:gens grouped)
                          (:ops grouped))
    (aggregation-assertions! (:buffers grouped)
                             (:aggs grouped)
                             options)))

(defn query-signature?
  "Accepts the normalized return vector of a Cascalog form and returns
  true if the return vector is from a subquery, false otherwise. (A
  predicate macro would trigger false, for example.)"
  [vars]
  (not (some v/selector? vars)))

;; this is the root of the tree, used to account for all variables as
;; they're built up.

(p/defnode Merge [sources]
  zip/TreeNode
  (branch? [_] true)
  (children [_] sources)
  (make-node [node children]
             (assoc node :sources children)))

(p/defnode TailStruct [node ground? available-fields operations options]
  algebra/Semigroup
  (plus [l r]
        (assert (and (:ground? l) (:ground? r))
                "Both tails must be ground.")
        (assoc l
          :node (->Merge [(:node l) (:node r)])
          :operations (union (:operations l)
                             (:operations r))))
  zip/TreeNode
  (branch? [_] true)
  (children [_] [node])
  (make-node [this children]
             (assoc this :node (first children))))

(def tail?
  "Returns true if the supplied item is a TailStruct, false
  otherwise."
  (partial instance? TailStruct))

;; ExistenceNode is the same as a GeneratorSet, basically.
(p/defnode ExistenceNode [source output-field]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [node children]
             (assoc node :source (first children))))

;; For function applications.
(p/defnode Application [source operation]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [node children]
             (assoc node :source (first children))))

(p/defnode Rename [source input output]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [node children]
             (assoc node :source (first children))))

(p/defnode Projection [source fields]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [node children]
             (assoc node :source (first children))))

;; For filters.
(p/defnode FilterApplication [source filter]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [node children]
             (assoc node :source (first children))))

;; TODO: Potentially add aggregations into the join. This node
;; combines many sources.
(p/defnode Join [sources join-fields type-seq options]
  zip/TreeNode
  (branch? [_] true)
  (children [_] sources)
  (make-node [node children]
             (assert (= (count children)
                        (count type-seq)))
             (assoc node :sources children)))

(p/defnode Unique [source fields options]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [node children]
             (assoc node :source (first children))))

;; Build one of these from many aggregators.
(p/defnode Grouping [source aggregators grouping-fields options]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [node children]
             (assoc node :source (first children))))

(defn existence-field
  "Returns true if this location directly descends from an
  ExistenceNode, false otherwise. Short-circuits at any merge."
  [node]
  (loop [loc (zip/cascalog-zip node)]
    (if (czip/branch? loc)
      (let [node (czip/node loc)]
        (if (instance? ExistenceNode node)
          (:output-field node)
          (let [child-count (count (czip/children loc))]
            (if-not (> child-count 1)
              (and (czip/down loc)
                   (recur (czip/down loc))))))))))

(def existence-branch?
  (comp boolean existence-field))

;; ## Operation Application

(defn op-allowed?
  "An operation can be applied to a tail if all of the following
  conditions apply:

  - It only consumes fields that are available in the supplied
  TailStruct,

  - It's a filter (or the branch is NOT a GeneratorSet)

  - It only consumes ground variables (or the generator itself is
  ground)"
  [{:keys [ground? available-fields node]} op]
  (let [set-branch?   (existence-branch? node)
        available-set (set available-fields)
        infields-set  (set (filter v/cascalog-var? (:input op)))
        all-ground?   (every? v/ground-var? infields-set)]
    (and (or (instance? FilterOperation op)
             (not set-branch?))
         (subset? infields-set available-set)
         (or all-ground? ground?))))

;; TODO: If we have these, do we really need the extra Operation and
;; FilterOperation wrapping in predicate?

(defprotocol IApplyToTail
  (accept? [this tail]
    "Returns true if this op can be applied to the current tail")

  (apply-to-tail [this tail]
    "Accepts a tail and performs some modification on that tail,
    returning a new tail."))

(defn apply-equality-ops
  "Accepts a TailStruct instance and a sequence of pairs of input
  variables, and applies an equality filter for every pair."
  [tail equality-pairs]
  (reduce (fn [tail equality-pair]
            (apply-to-tail (p/to-predicate = equality-pair nil) tail))
          tail
          equality-pairs))

(defn prepare-operation
  "When an operation produces fields that are already present in the
  tail, this is interpreted as an implicit filter against the existing
  values. This function accepts an operation and a TailStruct and
  returns a sequence of all pairs of output variable substitutions,
  plus a new operation with output fields swapped as necessary"
  [op tail]
  (if-let [duplicates (not-empty
                       (intersection (set (:output op))
                                     (set (:available-fields tail))))]
    (let [[eq-pairs output]
          (s/unweave (mapcat (fn [v]
                               (if-not (contains? duplicates v)
                                 [[] v]
                                 (let [uniqued (v/uniquify-var v)]
                                   [[v uniqued] uniqued])))
                             (:output op)))]
      [(filter not-empty eq-pairs)
       (assoc op :output output)])
    [[] op]))

(defn chain [tail f]
  (-> tail
      (update-in [:node] f)))

(extend-protocol IApplyToTail
  Object
  (accept? [_ tail] false)

  Operation
  (accept? [op tail] (op-allowed? tail op))
  (apply-to-tail [op tail]
    (let [[eq-pairs op] (prepare-operation op tail)]
      (-> tail
          (chain #(->Application % op))
          (update-in [:available-fields] #(concat % (:output op)))
          (apply-equality-ops eq-pairs))))

  FilterOperation
  (accept? [op tail] (op-allowed? tail op))
  (apply-to-tail [op tail]
    (chain tail #(->FilterApplication % op))))

(comment
  "TODO: Make a test.
This won't work in distributed mode because of the ->Record functions."
  (let [good-op (p/->FilterOperation = [10 "?a"])
        bad-op  (p/->FilterOperation = [10 "?b"])
        tail (map->TailStruct {:ground? true
                               :available-fields ["?a" "!z"]
                               :node [[1] [2]]})]
    (prn (accept? good-op tail))
    (prn (accept? bad-op tail))))

(defn prefer-filter [op]
  (if (instance? FilterOperation op)
    -1 0))

(defn add-ops-fixed-point
  "Adds operations to tail until can't anymore. Returns new tail and
  any unapplied operations."
  [tail]
  (let [[candidates failed] (s/separate #(accept? % tail)
                                        (:operations tail))]
    (if-not (seq candidates)
      tail
      (let [[operation & remaining] (sort-by prefer-filter candidates)]
        (recur (apply-to-tail operation (assoc tail :operations
                                               (concat remaining failed))))))))

;; ## Join Field Detection

(defn tail-fields-intersection [& tails]
  (->> tails
       (map (comp set :available-fields))
       (apply intersection)))

(defn joinable?
  "Returns true if the supplied tail can be joined with the supplied
  join fields, false otherwise.

  A join works if the join fields are all available in the given tail
  AND the tail's either fully ground, or every non-join variable is
  unground."
  [tail joinfields]
  (let [join-set   (set joinfields)
        tailfields (set (:available-fields tail))]
    (and (subset? join-set tailfields)
         (or (:ground? tail)
             (every? v/unground-var?
                     (difference tailfields join-set))))))

(defn find-join-fields [l r]
  (let [join-set (tail-fields-intersection l r)]
    (if (and (joinable? l join-set)
             (joinable? r join-set))
      join-set
      [])))

(defn maximal-join
  "Returns the between the two generators with the largest
  intersection of joinable fields."
  [tail-seq]
  (let [join-fields (map (fn [[t1 t2]] (find-join-fields t1 t2))
                         (s/all-pairs tail-seq))]
    (apply max-key count join-fields)))

(defn select-join
  "Returns the join fields that will join the maximum number of fields
  at a time. If the search fails, select-join throws.

   This is unoptimal. It's better to rewrite this as a search problem
   to find optimal joins."
  [tails]
  (or (not-empty (maximal-join tails))
      (u/throw-illegal "Unable to join predicates together")))

(defn attempt-join
  "Attempt to reduce the supplied set of tails by joining."
  [tails options]
  (let [max-join (select-join tails)
        [join-set remaining] (s/separate #(joinable? % max-join) tails)
        ;; All join fields survive from normal generators; from
        ;; generator-as-set generators, only the field we need to
        ;; filter gets through.
        available-fields (distinct
                          (mapcat (fn [tail]
                                    (if-let [ef (existence-field tail)]
                                      [ef]
                                      (:available-fields tail)))
                                  join-set))
        join-node (->Join (map :node join-set)
                          (vec max-join)
                          (map (fn [g]
                                 [(:available-fields g)
                                  (if-not (:ground? g)
                                    :outer
                                    (or (existence-field g)
                                        :inner))])
                               join-set)
                          options)
        new-ops (when-let [ops (seq (map (comp set :operations) join-set))]
                  (apply intersection ops))]
    (conj remaining (->TailStruct join-node
                                  (s/some? :ground? join-set)
                                  available-fields
                                  (vec new-ops)
                                  {}))))

;; ## Aggregation Operations
;;
;; The following operations deal with Cascalog's aggregations. I think
;; we can replace all of this by delegating out to the GroupBy
;; implementation in operations.

(defn grouping-input
  "These are the operations that go into the aggregators."
  [aggs sort-fields]
  (->> aggs
       (map #(set (:input %)))
       (apply union (set sort-fields))
       (vec)))

(defn grouping-output
  "Returns the union of all grouping fields and all outputs for every
  aggregation field. These are the only fields available after the
  aggregation."
  [aggs grouping-fields]
  (->> aggs
       (map #(set (:output %)))
       (apply union (set grouping-fields))
       (vec)))

(defn projection-input
  "These are the fields that go into a projection."
  [aggs grouping-fields options]
  (let [sort-fields (:sort options)]
    (->> aggs
        (map #(set (:input %)))
        (apply union (set (concat grouping-fields sort-fields)))
        (vec))))

(defn validate-aggregation!
  "Makes sure that all fields are available for the aggregation."
  [tail aggs options]
  (let [required-input (grouping-input aggs (:sort options))]
    (when-let [missing-fields (seq
                               (difference (set required-input)
                                           (set (:available-fields tail))))]
      (u/throw-runtime "Can't apply all aggregators. These fields are missing: "
                       missing-fields))))

(defn build-agg-tail
  [tail aggs grouping-fields options]
  (if (empty? aggs)
    (if (:distinct options)
      (chain tail #(->Unique % grouping-fields options))
      tail)
    (let [total-fields (grouping-output aggs grouping-fields)
          projection-fields (projection-input aggs grouping-fields options)]
      (validate-aggregation! tail aggs options)
      (-> tail
          (chain #(->Projection % projection-fields))
          (chain #(->Grouping % aggs grouping-fields options))
          (assoc :available-fields total-fields)))))

(defn merge-tails
  "The first call begins with a bunch of generator tails, each with a
   list of operations that could be applied. Based on the op-allowed
   logic, these tails try to consume as many operations as possible
   before giving up at a fixed point."
  [tails options]
  (assert (not-empty tails) "Tails required in merge-tails.")
  (if (= 1 (count tails))
    (add-ops-fixed-point (assoc (first tails) :ground? true))
    (let [tails (map add-ops-fixed-point tails)]
      (recur (attempt-join tails options) options))))

(defn initial-tails
  "Builds up a sequence of tail structs from the supplied generators
  and operations."
  [generators operations]
  (->> generators
       (map (fn [gen]
              (let [[gen node] (if (instance? GeneratorSet gen)
                                 [(:generator gen)
                                  (->ExistenceNode (:generator gen)
                                                   (:join-set-var gen))]
                                 [gen gen])]
                (->TailStruct node
                              (v/fully-ground? (:fields gen))
                              (:fields gen)
                              operations
                              {}))))))

(defn validate-projection!
  [remaining-ops needed available]
  (when-not (empty? remaining-ops)
    (u/throw-runtime (str "Could not apply all operations: " (pr-str remaining-ops))))
  (let [want-set (set needed)
        have-set (set available)]
    (when-not (subset? want-set have-set)
      (let [inter (intersection have-set want-set)
            diff  (difference want-set have-set)]
        (u/throw-runtime (str "Only able to build to " (vec inter)
                              " but need " (vec needed)
                              ". Missing " (vec diff)))))))

(defn split-outvar-constants
  "Accepts a sequence of output variables and returns a 2-vector:

  [new-outputs, [seq-of-new-raw-predicates]]

  By creating a new output predicate for every constant in the output
  field."
  [output]
  (let [[_ cleaned] (v/replace-dups output)]
    (reduce (fn [[new-output pred-acc] [v clean]]
              (if (v/cascalog-var? v)
                (if (= v clean)
                  [(conj new-output v) pred-acc]
                  [(conj new-output clean)
                   (conj pred-acc
                         (p/RawPredicate. = [v clean] nil))])
                (let [newvar (v/gen-nullable-var)]
                  [(conj new-output newvar)
                   (conj pred-acc
                         (if (or (fn? v)
                                 (u/multifn? v))
                           (p/RawPredicate. v [newvar] nil)
                           (p/RawPredicate. = [v newvar] nil)))])))
            [[] []]
            (map vector output cleaned))))

(defn validate-generator-set!
  "GeneratorSets can't be unground, ever."
  [input output]
  (when (not-empty input)
    (when (> (count output) 1)
      (u/throw-illegal "Only one output variable allowed in a generator-as-set."))
    (when-let [unground (not-empty (filter v/unground-var? (concat input output)))]
      (u/throw-illegal (str "Can't use unground vars in generators-as-sets. "
                            (vec unground)
                            " violate(s) the rules.\n\n")))))

(defn expand-outvars [{:keys [op input output] :as pred}]
  (when (p/can-generate? op)
    (validate-generator-set! input output))
  (let [[cleaned new-preds] (split-outvar-constants output)]
    (concat new-preds
            (if (and (not-empty input) (p/can-generate? op))
              (expand-outvars
               (p/RawPredicate. (p/GeneratorSet. op (first cleaned))
                                []
                                input))
              [(assoc pred :output cleaned)]))))

(defn project [tail fields]
  (let [fields (s/collectify fields)
        available (:available-fields tail)]
    (u/safe-assert (subset? (set fields)
                            (set available))
                   (format "Cannot select %s from %s."
                           fields
                           available))
    (-> tail
        (chain #(->Projection % fields))
        (assoc :available-fields fields))))

(defn rename [tail fields]
  (let [fields (s/collectify fields)
        available (:available-fields tail)]
    (u/safe-assert (= (count available)
                      (count fields))
                   (format
                    "Must rename to the same number of fields. %s to %s is invalid."
                    fields
                    available))
    (-> tail
        (chain #(->Rename % available fields))
        (assoc :available-fields fields)
        (assoc :ground? (v/fully-ground? fields)))))

(defn necessary-ops
  "Return only operations whose outvars intersect with necessary-fields"
  [necessary-fields ops]
  (filter #(seq (clojure.set/intersection
                   (set (:output %))
                   (set necessary-fields)))
          ops))

(defn fixed-point-prune
  "Handle pruning chained operation"
  [base-necessary-fields ops-invar-fields ops]
  (let [next-ops (necessary-ops (concat base-necessary-fields
                                        ops-invar-fields)
                                ops)
        next-invars (mapcat :input next-ops)]
    (if (= ops next-ops)
      next-ops
      (fixed-point-prune base-necessary-fields next-invars next-ops))))

(defn prune-operations
  "Remove non-generator & non-filter operations whose outvar(s) is not used, i.e., when the outvar(s)
  do *not* intersect with the query's out-fields, generators outvars (for joins), invars of other operations,
  and :sort option. Additionally, do not prune operations if a no-input operator exists"
  [fields grouped options]
  (if (some #(empty? (:input %)) (concat (grouped Operation)
                                         (grouped FilterOperation)
                                         (grouped Aggregator)))
    (concat (grouped Operation)
            (grouped FilterOperation))
    (let [generators (concat (grouped Generator)
                             (grouped GeneratorSet))
          base-necessary-fields (->> (concat (grouped FilterOperation)
                                             (grouped Aggregator))
                                     (mapcat :input)
                                     (concat fields)
                                     (concat (mapcat :fields generators))
                                     (concat (:sort options)))
          ops-invar-fields (mapcat :input (grouped Operation))
          pruned-operations (fixed-point-prune base-necessary-fields
                                               ops-invar-fields
                                               (grouped Operation))]
      (concat pruned-operations
              (grouped FilterOperation)))))


(defn build-rule
  [{:keys [fields predicates options] :as input}]
  (let [[nodes expanded] (s/separate #(tail? (:op %)) predicates)
        grouped (->> expanded
                     (map (partial p/build-predicate options))
                     (group-by type))
        generators (concat (grouped Generator)
                           (grouped GeneratorSet))
        operations (prune-operations fields grouped options)
        aggs       (grouped Aggregator)
        tails      (concat (initial-tails generators operations)
                           (map (fn [{:keys [op output]}]
                                  (-> op
                                      (rename output)
                                      (assoc :operations operations)))
                                nodes))
        joined     (merge-tails tails options)
        grouping-fields (filter
                         (set (:available-fields joined))
                         fields)
        agg-tail (build-agg-tail joined aggs grouping-fields options)
        {:keys [operations available-fields] :as tail} (add-ops-fixed-point agg-tail)]
    (validate-projection! operations fields available-fields)
    (-> (project tail fields)
        (assoc :options options))))

;; ## Predicate Parsing
;;
;; Before compilation, all predicates are normalized down to clojure
;; predicates.
;;
;; Query compilation steps are as follows:
;;
;; 1. Desugar all of the argument selectors (remember positional!)
;; 2. Normalize all predicates
;; 3. Expand predicate macros
;;
;; The result of this is a RawSubquery instance with RawPredicates,
;; output fields and options.

(defn build-query
  [output-fields raw-predicates]
  (let [[options predicates] (opts/extract-options raw-predicates)
        expanded (mapcat expand-outvars predicates)]
    (validate-predicates! expanded options)
    (p/RawSubquery. output-fields expanded options)))

(defn prepare-subquery [output-fields raw-predicates]
  (let [output-fields (v/sanitize output-fields)
        raw-predicates (mapcat p/normalize raw-predicates)]
    {:output-fields output-fields
     :predicates raw-predicates}))

(defn parse-subquery
  "Parses predicates and output fields and returns a proper subquery."
  [output-fields raw-predicates]
  (let [{output-fields :output-fields
         raw-predicates :predicates}
        (prepare-subquery output-fields raw-predicates)]
    (if (query-signature? output-fields)
      (build-rule
       (build-query output-fields raw-predicates))
      (let [parsed (parse-variables output-fields :<)]
        (pm/build-predmacro (:input parsed)
                            (:output parsed)
                            raw-predicates)))))

(defmacro <-
  "Constructs a query or predicate macro from a list of
  predicates. Predicate macros support destructuring of the input and
  output variables."
  [outvars & predicates]
  `(v/with-logic-vars ~(cons outvars (map rest predicates))
     (parse-subquery ~outvars [~@(map vec predicates)])))

(defn parse-exec-args
  "Accept a sequence of (maybe) string and other items and returns a
  vector of [theString or \"\", [other items]]."
  [[f & rest :as args]]
  (if (string? f)
    [f rest]
    ["" args]))

(defprotocol IOutputFields
  (get-out-fields [_] "Get the fields of a generator."))

(extend-protocol IOutputFields

  TailStruct
  (get-out-fields [tail]
    (:available-fields tail))

  Subquery
  (get-out-fields [sq]
    (get-out-fields (.getCompiledSubquery sq))))

(defprotocol INumOutFields
  (num-out-fields [_]))

(extend-protocol INumOutFields
  Subquery
  (num-out-fields [sq]
    (count (seq (.getOutputFields sq))))

  clojure.lang.ISeq
  (num-out-fields [x]
    (count (s/collectify (first x))))

  clojure.lang.IPersistentVector
  (num-out-fields [x]
    (count (s/collectify (peek x))))

  TailStruct
  (num-out-fields [x]
    (count (:available-fields x))))

(defprotocol ISelectFields
  (select-fields [gen fields]
    "Select fields of a named generator.

  Example:
  (<- [?a ?b ?sum]
      (+ ?a ?b :> ?sum)
      ((select-fields generator [\"?a\" \"?b\"]) ?a ?b))"))

(extend-protocol ISelectFields
  TailStruct
  (select-fields [sq fields]
    (project sq fields))

  Subquery
  (select-fields [sq fields]
    (select-fields (.getCompiledSubquery sq) fields)))
