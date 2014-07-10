(ns cascalog.cascading.platform
  (:require [jackknife.seq :as s]
            [jackknife.core :as u]
            [cascalog.cascading.operations :as ops]
            [cascalog.cascading.util :as casc]
            [cascalog.cascading.types :as types]
            [cascalog.logic.predicate :as p]
            [cascalog.logic.def :as d]
            [cascalog.logic.parse :as parse]
            [cascalog.logic.algebra :refer (sum)]
            [cascalog.logic.zip :as zip]
            [cascalog.logic.fn :as serfn]
            [cascalog.logic.vars :as v]
            [cascalog.logic.parse :as parse]
            [cascalog.logic.platform :refer (generator IGenerator IPlatform)])
  (:import [cascading.pipe Each Every]
           [cascading.tuple Fields]
           [cascading.operation Function Filter]
           [cascalog.aggregator CombinerSpec ClojureMonoidAggregator
            ClojureParallelAggregator]
           [cascalog CascalogFunction ClojureBufferCombiner
            CascalogFunctionExecutor CascadingFilterToFunction
            CascadingFunctionWrapper CascalogBuffer
            CascalogBufferExecutor CascalogAggregator
            CascalogAggregatorExecutor ParallelAgg]
           [cascalog.logic.parse TailStruct Projection Application
            FilterApplication Grouping Join ExistenceNode
            Unique Merge Rename]
           [cascalog.logic.predicate RawSubquery FilterOperation
            Operation Aggregator]
           [cascalog.cascading.operations IAggregateBy IAggregator
            Inner Outer Existence]
           [cascalog.logic.def ParallelAggregator ParallelBuffer Prepared]
           [cascalog.cascading.types ClojureFlow]
           [jcascalog Predicate]))

(defn- init-pipe-name [options]
  (or (:name (:trap options))
      (u/uuid))) 

(defn- init-trap-map [options]
  (if-let [trap (:trap options)]
    {(:name trap) (types/to-sink (:tap trap))}
    {}))

(defrecord CascadingPlatform []
  IPlatform
  (pgenerator? [_ x]
    (satisfies? IGenerator x))

  (pgenerator [_ gen fields options]
    (-> (generator gen)
        (update-in [:trap-map] #(merge % (init-trap-map options)))
        (ops/rename-pipe (init-pipe-name options))
        (ops/rename* fields)
        ;; All generators if the fields aren't ungrounded discard null values
        (ops/filter-nullable-vars fields))))

(extend-protocol p/IRawPredicate
  Predicate
  (normalize [p]
    (p/normalize (into [] (.toRawCascalogPredicate p)))))

;; ## Allowed Predicates

(defmethod p/to-predicate Filter
  [op input output]
  (u/safe-assert (#{0 1} (count output)) "Must emit 0 or 1 fields from filter")
  (if (empty? output)
    (FilterOperation. op input)
    (Operation. op input output)))

(defmethod p/to-predicate Function
  [op input output]
  (Operation. op input output))

(defmethod p/to-predicate ParallelAgg
  [op input output]
  (Aggregator. op input output))

(defmethod p/to-predicate CascalogBuffer
  [op input output]
  (Aggregator. op input output))

;; ## Query Execution
;;
;; TODO: Add a dynamic variable that holds an execution context. The
;; implementation below is a planner for Hadoop. We should be able to
;; write planners equally well for other systems, like Spark or Storm.

;; Currently predicate macros are expanded in build-rule, in
;; rules.clj. By the time we get here, let's assume that everything
;; has been properly parsed and expanded.

(defn assem*
  [fun]
  (fn [gen input output]
    (ops/logically gen input output
                   (fn [gen in out]
                     (fun gen in out)))))

(defmacro assem [argv & more]
  `(assem* (ops/assembly ~argv ~@more)))

(defn filter-assem*
  [fun]
  (fn [gen input]
    (ops/logically gen input []
                   (fn [gen in _] (fun gen in)))))

(defmacro filter-assem [argv & more]
  `(filter-assem* (ops/assembly ~argv ~@more)))

(extend-protocol p/ICouldFilter
  cascading.operation.Filter
  (filter? [_] true))

(defmulti op-cascading
  (fn [op gen input output]
    (type op)))

(defmulti filter-cascading
  (fn [op gen input]
    (type op)))

(defmulti agg-cascading
  (fn [op input output]
    (type op)))

(defmethod op-cascading Filter
  [op gen input output]
  ((assem
    [in out]
    (ops/each #(CascadingFilterToFunction. (first %) op) in out))
   gen input output))

(defmethod op-cascading Function
  [op gen input output]

  ((assem [in out]
          (ops/each #(CascadingFunctionWrapper. % op) in out))
   gen input output))

(defmethod op-cascading CascalogFunction
  [op gen input output]
  ((assem [in out] (ops/each #(CascalogFunctionExecutor. % op) in out))
   gen input output))

(defmethod op-cascading ::d/map
  [op gen input output]
  ((assem [in out] (ops/map* op in out))
   gen input output))

(defmethod op-cascading ::d/mapcat
  [op gen input output]
  ((assem [in out] (ops/mapcat* op in out))
   gen input output))

(defmethod filter-cascading ::d/filter
  [op gen input]
  ((filter-assem [in] (ops/filter* op in))
   gen input))

(defmethod filter-cascading cascading.operation.Filter
  [op gen input]
  ((filter-assem [in] (ops/add-op #(Each. % in op)))
   gen input))

(defmethod agg-cascading ::d/buffer
  [op input output]
  (ops/buffer op input output))

(defmethod agg-cascading ::d/bufferiter
  [op input output]
  (ops/bufferiter op input output))

(defmethod agg-cascading ::d/aggregate
  [op input output]
  (ops/agg op input output))

(defmethod agg-cascading ::d/combiner
  [op input output]
  (ops/parallel-agg op input output))

(defmethod agg-cascading ParallelAggregator
  [op input output]
  (ops/parallel-agg (:combine-var op) input output
                    :init-var (:init-var op)
                    :present-var (:present-var op)))

(defmethod agg-cascading ParallelAgg
  [op input output]
  (ops/parallel-agg (serfn/fn [l r]
                      (-> op
                          (.combine (s/collectify l)
                                    (s/collectify r))))
                    input output
                    :init-var (serfn/fn [x]
                                (.init op (s/collectify x)))))

(defmethod agg-cascading CascalogBuffer
  [op input output]
  (reify ops/IBuffer
    (add-buffer [_ pipe]
      (Every. pipe (casc/fields input)
              (CascalogBufferExecutor. (casc/fields output) op)))))

(defmethod agg-cascading CascalogAggregator
  [op input output]
  (reify ops/IAggregator
    (add-aggregator [_ pipe]
      (Every. pipe (casc/fields input)
              (CascalogAggregatorExecutor. (casc/fields output) op)))))

(defn opt-seq
  "Takes a Cascalog option map and returns a sequence of option, value
  pairs suitable " [options]
  (->> (assoc options
         :sort-fields (:sort options)
         :reverse? (boolean (:reverse options)))
       (filter (comp (complement nil?) second))
       (apply concat)))

(defprotocol IRunner
  (to-generator [item]))

;; TODO: Generator should just be a projection.
;; TODO: Add a validation here that checks if this thing is a
;; generator and sends a proper error message otherwise.
(extend-protocol IRunner
  Object
  (to-generator [x]
    (generator x))

  cascalog.logic.predicate.Generator
  (to-generator [{:keys [gen]}] gen)

  Application
  (to-generator [{:keys [source operation]}]
    (let [{:keys [op input output]} operation]
      (op-cascading op source input output)))

  FilterApplication
  (to-generator [{:keys [source filter]}]
    (let [{:keys [op input]} filter]
      (filter-cascading op source input)))

  ExistenceNode
  (to-generator [{:keys [source]}] source)

  Join
  (to-generator [{:keys [sources join-fields type-seq options]}]
    (-> (ops/cascalog-join (map (fn [source [available type]]
                                  (condp = type
                                    :inner (ops/Inner. source available)
                                    :outer (ops/Outer. source available)
                                    (ops/Existence. source available type)))
                                sources type-seq)
                           join-fields
                           (opt-seq options))
        (ops/rename-pipe (.getName (:pipe (first sources))))))

  Grouping
  (to-generator [{:keys [source aggregators grouping-fields options]}]
    (if-let [bufs (not-empty
                   (filter #(instance? ParallelBuffer (:op %)) aggregators))]
      (do (assert (= (count aggregators) 1)
                  "Only one buffer allowed per subquery.")
          (let [{:keys [op input output]} (first bufs)
                {:keys [init-var combine-var present-var
                        num-intermediate-vars-fn buffer-var]} op
                temps (v/gen-nullable-vars
                       (num-intermediate-vars-fn input output))
                spec (-> (CombinerSpec. combine-var)
                         (.setPrepareFn init-var)
                         (.setPresentFn present-var))
                source (-> source
                           (ops/add-op #(Each. % Fields/ALL
                                               (ClojureBufferCombiner.
                                                (casc/fields grouping-fields)
                                                (casc/fields (:sort options))
                                                (casc/fields input)
                                                (casc/fields temps)
                                                spec))))]
            (apply ops/group-by*
                   source
                   grouping-fields
                   [(ops/buffer buffer-var temps output)]
                   (opt-seq options))))
      (let [aggs (map (fn [{:keys [op input output]}]
                        (agg-cascading op input output))
                      aggregators)]
        (apply ops/group-by*
               source grouping-fields aggs (opt-seq options)))))

  Unique
  (to-generator [{:keys [source fields options]}]
    (apply ops/unique source fields (opt-seq options)))

  Merge
  (to-generator [{:keys [sources]}]
    (sum sources))

  Projection
  (to-generator [{:keys [source fields]}]
    (-> source
        (ops/select* fields)
        (ops/filter-nullable-vars fields)))

  Rename
  (to-generator [{:keys [source fields]}]
    (-> source
        (ops/rename* fields)
        (ops/filter-nullable-vars fields)))

  TailStruct
  (to-generator [item]
    (:node item)))

;; TODO: This needs to move back into the logic DSL. We need a dynamic
;; variable with the "runner", which will need to supply a
;; "to-generator" method.

(defn compile-query [query]
  (zip/postwalk-edit
   (zip/cascalog-zip query)
   identity
   (fn [x _] (to-generator x))
   :encoder (fn [x]
              (or (:identifier x) x))))

(extend-protocol IGenerator
  TailStruct
  (generator [sq]
    (compile-query sq))

  RawSubquery
  (generator [sq]
    (generator (parse/build-rule sq))))

(comment
  "MOVE these to tests."
  (require '[cascalog.logic.parse :refer (<-)]
           '[cascalog.cascading.flow :refer (all-to-memory to-memory graph)])

  (def cross-join
    (<- [:>] (identity 1 :> _)))

  (let [sq (<- [?squared ?squared-minus ?x ?sum]
               ([1 2 3] ?x)
               (* ?x ?x :> ?squared)
               (- ?squared 1 :> ?squared-minus)
               ((d/parallelagg* +) ?squared :> ?sum))]
    (to-memory sq))

  (let [sq (<- [?x ?y]
               ([1 2 3] ?x)
               ([1 2 3] ?y)
               (cross-join)
               (* ?x ?y :> ?z))]
    (to-memory sq))

  (let [x (<- [?x ?y :> ?z]
              (* ?x ?x :> 10)
              (* ?x ?y :> ?z))
        sq (<- [?a ?b ?z]
               ([[1 2 3]] ?a)
               (x ?a ?a :> 4)
               ((d/bufferop* +) ?a :> ?z)
               ((d/mapcatop* +) ?a 10 :> ?b))]
    (clojure.pprint/pprint (build-rule sq))))
