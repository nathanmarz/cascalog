(ns cascalog.cascading.platform
  (:require [jackknife.seq :as s]
            [cascalog.cascading.operations :as ops]
            [cascalog.logic.predicate :as p]
            [cascalog.logic.def :as d]
            [cascalog.logic.parse :as parse]
            [cascalog.logic.algebra :refer (sum)]
            [cascalog.logic.zip :as zip]
            [cascalog.logic.parse :as parse]
            [cascalog.cascading.types :refer (IGenerator generator)])
  (:import [cascading.pipe Each Every]
           [cascalog CascalogFunction
            CascalogFunctionExecutor CascadingFilterToFunction
            CascalogBuffer CascalogBufferExecutor CascalogAggregator
            CascalogAggregatorExecutor ClojureParallelAgg ParallelAgg]
           [cascalog.logic.parse TailStruct Projection Application
            FilterApplication Grouping Join ExistenceNode RawSubquery
            Unique Merge]))

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

(defmethod op-cascading cascading.operation.Filter
  [op gen input output]
  ((assem
    [in out]
    (ops/each #(CascadingFilterToFunction. (first %) op) in out))
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

(defprotocol IRunner
  (to-generator [item]))

;; TODO: Generator should just be a projection.
(extend-protocol IRunner
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
  (to-generator [{:keys [sources join-fields type-seq]}]
    (ops/cascalog-join (map (fn [source [available type]]
                              (condp = type
                                :inner (ops/->Inner source available)
                                :outer (ops/->Outer source available)
                                (ops/->Existence source available type)))
                            sources type-seq)
                       join-fields))

  Grouping
  (to-generator [{:keys [source aggregators grouping-fields options]}]
    (let [aggs (map (fn [{:keys [op input output]}]
                      ;; We pass the options in here to support
                      ;; aggregators that might need to sort, etc.
                      (op input output))
                    aggregators)
          options (assoc options
                    :sort-fields (:sort options)
                    :reverse? (boolean (:reverse options)))
          opts (->> options
                    (filter (comp (complement nil?) second))
                    (s/flatten))]
      (apply ops/group-by* source grouping-fields aggs opts)))

  Unique
  (to-generator [{:keys [source fields options]}]
    (to-generator
     (-> source
         (parse/->Grouping
          [(p/->Aggregator (constantly (ops/unique-aggregator))
                           fields
                           fields)]
          fields
          options))))

  Merge
  (to-generator [{:keys [sources]}]
    (sum sources))

  Projection
  (to-generator [{:keys [source fields]}]
    (-> source
        (ops/select* fields)
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
   (fn [x _]
     (to-generator x))))

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
