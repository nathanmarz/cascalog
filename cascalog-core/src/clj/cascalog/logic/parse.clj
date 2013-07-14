(ns cascalog.logic.parse
  (:require [clojure.string :refer (join)]
            [clojure.set :refer (difference intersection union subset?)]
            [clojure.walk :refer (postwalk)]
            [clojure.zip :as czip]
            [jackknife.core :as u :refer (throw-illegal throw-runtime)]
            [jackknife.seq :as s]
            [cascalog.logic.def :as d :refer (bufferop? aggregateop?)]
            [cascalog.logic.fn :refer (search-for-var)]
            [cascalog.logic.vars :as v]
            [cascalog.logic.zip :as zip]
            [cascalog.logic.predicate :as p]
            [cascalog.logic.options :as opts]
            [cascalog.cascading.types :refer (generator?)])
  (:import [jcascalog Predicate PredicateMacro PredicateMacroTemplate]
           [cascalog.logic.predicate Operation FilterOperation Aggregator
            Generator GeneratorSet]
           [clojure.lang IPersistentVector]))

(defprotocol IRawPredicate
  (normalize [_]
    "Returns a sequence of RawPredicate instances."))

;; Raw Predicate type.

(defrecord RawPredicate [op input output]
  IRawPredicate
  (normalize [p] [p]))

;; ## Predicate Macro Building Functions

;; TODO: expand should return a vector of RawPredicate instances.
;;
;; "expand" is called from "normalize" in cascalog.parse. The parsing
;;  code takes care of the recursive expansion needed on the results
;;  of a call to "expand".

(defprotocol IPredMacro
  (expand [_ input output]
    "Returns a sequence of vectors suitable to feed into
    cascalog.parse/normalize."))

(extend-protocol p/ICouldFilter
  cascalog.logic.parse.IPredMacro
  (filter? [_] true))

(extend-protocol IPredMacro
  ;; Predicate macro templates really should just extend this protocol
  ;; directly. getCompiledPredMacro calls into build-predmacro below
  ;; and returns a reified instance of IPredMacro.
  PredicateMacroTemplate
  (expand [p input output]
    ((.getCompiledPredMacro p) input output))

  ;; TODO: jCascalog shold just use these interfaces directly. If this
  ;; were the case, we wouldn't have to extend the protocol here.
  PredicateMacro
  (expand [p input output]
    (letfn [(to-fields [fields]
              (jcascalog.Fields. (or fields [])))]
      (-> p (.getPredicates (to-fields input)
                            (to-fields output))))))

(defn predmacro? [o]
  (satisfies? IPredMacro o))

;; kind of a hack, simulate using pred macros like filters

(defn use-as-filter?
  "If a predicate macro had a single output variable defined and you
  try to use it with no output variables, the predicate macro acts as
  a filter."
  [output-decl outvars]
  (and (empty? outvars)
       (sequential? output-decl)
       (= 1 (count output-decl))))

(defn predmacro*
  "Functional version of predmacro. See predmacro for details."
  [fun]
  (reify IPredMacro
    (expand [_ invars outvars]
      (fun invars outvars))))

(defmacro predmacro
  "A more general but more verbose way to create predicate macros.

   Creates a function that takes in [invars outvars] and returns a
   list of predicates. When making predicate macros this way, you must
   create intermediate variables with gen-nullable-var(s). This is
   because unlike the (<- [?a :> ?b] ...) way of doing pred macros,
   Cascalog doesn't have a declaration for the inputs/outputs.

   See https://github.com/nathanmarz/cascalog/wiki/Predicate-macros
  "
  [& body]
  `(predmacro* (fn ~@body)))

(defn validate-declarations!
  "Assert that the same variables aren't used on input and output when
  defining a predicate macro."
  [input-decl output-decl]
  (when (seq (intersection (set input-decl)
                           (set output-decl)))
    ;; TODO: ignore destructuring characters and check that no
    ;; constants are present.
    (throw-runtime (format
                    (str "Cannot declare the same var as "
                         "an input and output to predicate macro: %s %s")
                    input-decl output-decl))))

(defn build-predmacro
  "Build a predicate macro via input and output declarations. This
  function takes a sequence of declared inputs, a seq of declared
  outputs and a sequence of raw predicates. Upon use, any variable
  name not in the input or output declarations will be replaced with a
  random Cascalog variable (uniqued by appending a suffix, so nullable
  vs non-nullable will be maintained)."
  [input-decl output-decl raw-predicates]
  (validate-declarations! input-decl output-decl)
  (reify IPredMacro
    (expand [_ invars outvars]
      (let [outvars (if (use-as-filter? output-decl outvars)
                      [true]
                      outvars)
            replacement-m (s/mk-destructured-seq-map input-decl invars
                                                     output-decl outvars)
            update (memoize (fn [v]
                              (if (v/cascalog-var? v)
                                (replacement-m (str v) (v/uniquify-var v))
                                v)))]
        (->> raw-predicates
             (mapcat (fn [pred]
                       (map (fn [{:keys [input output] :as p}]
                              (-> p
                                  (assoc :input  (postwalk update input))
                                  (assoc :output (postwalk update output))))
                            (normalize pred)))))))))

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

(defn unweave
  "[1 2 3 4 5 6] -> [[1 3 5] [2 4 6]]"
  [coll]
  [(take-nth 2 coll) (take-nth 2 (rest coll))])

(defn to-map
  "Accepts a sequence of alternating [single-k, many-values] pairs and
  returns a map of k -> vals."
  [k? elems]
  (let [[keys vals] (unweave (partition-by k? elems))]
    (zipmap (map first keys) vals)))

(defn parse-variables
  "parses variables of the form ['?a' '?b' :> '!!c'] and returns a map
   of input variables, output variables, If there is no :>, defaults
   to selector-default."
  [vars default-selector]
  {:pre [(contains? #{:> :<} default-selector)]}
  (let [vars (cond (v/selector? (first vars)) vars
                   (some v/selector? vars) (cons :< vars)
                   :else (cons default-selector vars))
        {input :<< output :>>} (-> (to-map v/selector? vars)
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

(extend-protocol IRawPredicate
  Predicate
  (normalize [p]
    (normalize (into [] (.toRawCascalogPredicate p))))

  IPersistentVector
  (normalize [[op & rest]]
    (let [mk-single (fn [op in out]
                      [(->RawPredicate op (not-empty in) (not-empty out))])
          default (default-selector op)
          {:keys [input output]} (parse-variables rest (default-selector op))]
      (if (predmacro? op)
        (mapcat normalize (expand op input output))
        (mk-single op input output)))))

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
      (throw-illegal (str "Ungrounding vars must originate within a generator. "
                          extra-vars
                          " violate(s) the rules.")))
    (when (not-empty dups)
      (throw-illegal (str "Each ungrounding var can only appear once per query."
                          "The following are duplicated: "
                          dups)))))

(defn aggregation-assertions! [buffers aggs options]
  (when (and (not-empty aggs)
             (not-empty buffers))
    (throw-illegal "Cannot use both aggregators and buffers in same grouping"))
  ;; TODO: Move this into the fluent builder?
  (when (and (empty? aggs) (empty? buffers) (:sort options))
    (throw-illegal "Cannot specify a sort when there are no aggregators"))
  (when (> (count buffers) 1)
    (throw-illegal "Multiple buffers aren't allowed in the same subquery.")))

;; Output of the subquery, the predicates it contains and the options
;; in the subquery.
;;
(defrecord RawSubquery [fields predicates])

;; Printing Methods
;;
;; The following methods allow a predicate to print properly.

(defmethod print-method RawPredicate
  [{:keys [op input output]} ^java.io.Writer writer]
  (binding [*out* writer]
    (let [op (if (ifn? op)
               (let [op (or (::d/op (meta op)) op)]
                 (or (search-for-var op) op))
               op)]
      (print (str "(" op " "))
      (doseq [v (join " " input)]
        (print v))
      (when (not-empty output)
        (print " :> ")
        (doseq [v (join " " output)]
          (print v)))
      (println ")"))))

(defmethod print-method RawSubquery
  [{:keys [fields predicates]} ^java.io.Writer writer]
  (binding [*out* writer]
    (println "(<-" (vec fields))
    (doseq [pred predicates]
      (print "    ")
      (print-method pred writer))
    (println "    )")))

(defn validate-predicates! [preds]
  (let [grouped (group-by (fn [x]
                            (condp #(%1 %2) (:op x)
                              generator? :gens
                              bufferop? :buffers
                              aggregateop? :aggs
                              :ops))
                          preds)]
    (unground-assertions! (:gens grouped)
                          (:ops grouped))
    (aggregation-assertions! (:buffers grouped)
                             (:aggs grouped)
                             (first (opts/extract-options preds)))))

(defn query-signature?
  "Accepts the normalized return vector of a Cascalog form and returns
  true if the return vector is from a subquery, false otherwise. (A
  predicate macro would trigger false, for example.)"
  [vars]
  (not (some v/selector? vars)))

(comment
  (v/with-logic-vars
    (parse-subquery [?x ?y ?z]
                    [[[[1 2 3]] ?x]
                     [* ?x ?x :> ?y]
                     [* ?x ?y :> ?z]])))

;; this is the root of the tree, used to account for all variables as
;; they're built up.
(defrecord TailStruct [node ground? available-fields operations]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [node])
  (make-node [_ children]
    (->TailStruct (first children) ground? available-fields operations)))

;; ExistenceNode is the same as a GeneratorSet, basically.
(defrecord ExistenceNode [source output-field]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [_ children]
    (->ExistenceNode (first children) output-field)))

;; For function applications.
(defrecord Application [source operation]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [_ children]
    (->Application (first children) operation)))

(defrecord Projection [source fields]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [_ children]
    (->Projection (first children) fields)))

;; For filters.
(defrecord FilterApplication [source filter]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [_ children]
    (->FilterApplication (first children) filter)))

;; TODO: Potentially add aggregations into the join. This node
;; combines many sources.
(defrecord Join [sources join-fields type-seq]
  zip/TreeNode
  (branch? [_] true)
  (children [_] sources)
  (make-node [_ children]
    (assert (= (count children)
               (count type-seq)))
    (->Join children join-fields type-seq)))

(defrecord Merge [sources]
  zip/TreeNode
  (branch? [_] true)
  (children [_] sources)
  (make-node [_ children]
    (->Merge sources)))

(defrecord Unique [source fields options]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [_ children]
    (->Unique (first children) fields options)))

;; Build one of these from many aggregators.
(defrecord Grouping [source aggregators grouping-fields options]
  zip/TreeNode
  (branch? [_] true)
  (children [_] [source])
  (make-node [_ children]
    (->Grouping (first children) aggregators grouping-fields options)))

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
  (update-in tail [:node] f))

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
  "TODO: Make a test."
  (let [good-op (p/->FilterOperation = [10 "?a"])
        bad-op  (p/->FilterOperation = [10 "?b"])
        node (-> [[1] [2]]
                 (->ExistenceNode "fuck")
                 (->Application (p/->Operation * "a" "b")))
        tail (map->TailStruct {:ground? true
                               :available-fields ["?a" "!z"]
                               :node node})]
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
      (throw-illegal "Unable to join predicates together")))

(defn attempt-join
  "Attempt to reduce the supplied set of tails by joining."
  [tails]
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
                               join-set))
        new-ops (when-let [ops (seq (map (comp set :operations) join-set))]
                  (apply intersection ops))]
    (conj remaining (->TailStruct join-node
                                  (s/some? :ground? join-set)
                                  available-fields
                                  (vec new-ops)))))

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

(defn validate-aggregation!
  "Makes sure that all fields are available for the aggregation."
  [tail aggs options]
  (let [required-input (grouping-input aggs (:sort options))]
    (when-let [missing-fields (seq
                               (difference (set required-input)
                                           (set (:available-fields tail))))]
      (throw-runtime "Can't apply all aggregators. These fields are missing: "
                     missing-fields))))

(defn build-agg-tail
  [tail aggs grouping-fields options]
  (if (empty? aggs)
    (if (:distinct options)
      (chain tail #(->Unique % grouping-fields options))
      tail)
    (let [total-fields (grouping-output aggs grouping-fields)]
      (validate-aggregation! tail aggs options)
      (-> tail
          ;; TODO: Make this work properly.
          ;; (chain #(->Projection % total-fields))
          (chain #(->Grouping % aggs grouping-fields options))
          (assoc :available-fields total-fields)))))

(defn merge-tails
  "The first call begins with a bunch of generator tails, each with a
   list of operations that could be applied. Based on the op-allowed
   logic, these tails try to consume as many operations as possible
   before giving up at a fixed point."
  [tails]
  (if (= 1 (count tails))
    (add-ops-fixed-point (assoc (first tails) :ground? true))
    (let [tails (map add-ops-fixed-point tails)]
      (recur (attempt-join tails)))))

(defn initial-tails
  "Builds up a sequence of tail structs from the supplied generators
  and operations."
  [generators operations]
  (->> generators
       (map (fn [gen]
              (let [node (if (instance? GeneratorSet gen)
                           (->ExistenceNode (:generator gen)
                                            (:join-set-var gen))
                           gen)]
                (->TailStruct node
                              (v/fully-ground? (:fields gen))
                              (:fields gen)
                              operations))))))

(defn validate-projection!
  [remaining-ops needed available]
  (when-not (empty? remaining-ops)
    (throw-runtime (str "Could not apply all operations: " (pr-str remaining-ops))))
  (let [want-set (set needed)
        have-set (set available)]
    (when-not (subset? want-set have-set)
      (let [inter (intersection have-set want-set)
            diff  (difference have-set want-set)]
        (throw-runtime (str "Only able to build to " (vec inter)
                            " but need " (vec needed)
                            ". Missing " (vec diff)))))))

(defn split-outvar-constants
  "Accepts a sequence of output variables and returns a 2-vector:

  [new-outputs, [seq-of-new-raw-predicates]]

  By creating a new output predicate for every constant in the output
  field."
  [output]
  (reduce (fn [[new-output pred-acc] v]
            (if (v/cascalog-var? v)
              [(conj new-output v) pred-acc]
              (let [newvar (v/gen-nullable-var)]
                [(conj new-output newvar)
                 (conj pred-acc
                       (map->RawPredicate (if (or (fn? v)
                                                  (u/multifn? v))
                                            {:op v :input [newvar]}
                                            {:op = :input [v newvar]})))])))
          [[] []]
          output))

(defn expand-outvars [pred]
  (let [[cleaned new-preds] (split-outvar-constants (:output pred))]
    (concat new-preds
            [(assoc pred :output cleaned)])))

(defn build-rule
  [{:keys [fields predicates] :as input}]
  (let [[options predicates] (opts/extract-options predicates)
        grouped (->> predicates
                     (mapcat expand-outvars)
                     (map (partial p/build-predicate options))
                     (group-by type))
        generators (concat (grouped Generator)
                           (grouped GeneratorSet))
        operations (concat (grouped Operation)
                           (grouped FilterOperation))
        aggs       (grouped Aggregator)
        tails      (initial-tails generators operations)
        joined     (merge-tails tails)
        grouping-fields (seq (intersection
                              (set (:available-fields joined))
                              (set fields)))
        agg-tail (build-agg-tail joined aggs grouping-fields options)
        {:keys [operations available-fields] :as tail} (add-ops-fixed-point agg-tail)]
    (validate-projection! operations fields available-fields)
    (chain tail #(->Projection % fields))))

;; ## Predicate Parsing
;;
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
;; The result of this is a RawSubquery instance with RawPredicates
;; only inside.

(defn parse-subquery
  "Parses predicates and output fields and returns a proper subquery."
  [output-fields raw-predicates]
  (let [output-fields (v/sanitize output-fields)
        raw-predicates (mapcat normalize raw-predicates)]
    (if (query-signature? output-fields)
      (do (validate-predicates! raw-predicates)
          (build-rule
           (->RawSubquery output-fields raw-predicates)))
      (let [parsed (parse-variables output-fields :<)]
        (build-predmacro (:input parsed)
                         (:output parsed)
                         raw-predicates)))))

(defmacro <-
  "Constructs a query or predicate macro from a list of
  predicates. Predicate macros support destructuring of the input and
  output variables."
  [outvars & predicates]
  `(v/with-logic-vars
     (parse-subquery ~outvars [~@(map vec predicates)])))
