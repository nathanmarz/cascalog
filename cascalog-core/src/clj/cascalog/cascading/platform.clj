(ns cascalog.cascading.platform
  (:refer-clojure :exclude [run!])
  (:require [jackknife.seq :as s]
            [jackknife.core :as u]
            [cascalog.cascading.operations :as ops]
            [cascalog.cascading.util :as casc]
            [cascalog.cascading.types :as types]
            [cascalog.cascading.flow :as flow]
            [cascalog.logic.predicate :as pred]
            [cascalog.logic.def :as d]
            [cascalog.logic.parse :as parse]
            [cascalog.logic.algebra :refer (sum)]
            [cascalog.logic.fn :as serfn]
            [cascalog.logic.vars :as v]
            [cascalog.logic.platform :as p])
  (:import [cascading.pipe Each Every Pipe]
           [cascading.tuple Fields Tuple]
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
           [cascalog.cascading.types CascadingPlatform]
           [com.twitter.maple.tap MemorySourceTap]
           [cascading.tap Tap]
           [cascalog.cascading.tap CascalogTap]
           [jcascalog Predicate Subquery]))

(extend-protocol pred/IRawPredicate
  Predicate
  (normalize [p]
    (pred/normalize (into [] (.toRawCascalogPredicate p)))))

;; ## Allowed Predicates

(defmethod pred/to-predicate Filter
  [op input output]
  (u/safe-assert (#{0 1} (count output)) "Must emit 0 or 1 fields from filter")
  (if (empty? output)
    (FilterOperation. op input)
    (Operation. op input output)))

(defmethod pred/to-predicate Function
  [op input output]
  (Operation. op input output))

(defmethod pred/to-predicate ParallelAgg
  [op input output]
  (Aggregator. op input output))

(defmethod pred/to-predicate CascalogBuffer
  [op input output]
  (Aggregator. op input output))

(defmethod pred/to-predicate CascalogFunction
  [op input output]
  (Operation. op input output))

(defmethod pred/to-predicate CascalogAggregator
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

(extend-protocol pred/ICouldFilter
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
  (ops/parallel-agg (serfn/fn [& x]
                      (let [[l r] (split-at (/ (count x) 2) x)]
                       (-> op
                           (.combine (s/collectify l)
                                     (s/collectify r)))))
                    input output
                    :init-var (serfn/fn [& x]
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

;; TODO: Generator should just be a projection.
;; TODO: Add a validation here that checks if this thing is a
;; generator and sends a proper error message otherwise.

;; ## To Generators

(defmethod p/to-generator [CascadingPlatform Object]
  [x]
  (p/generator x))

(defmethod p/to-generator [CascadingPlatform cascalog.logic.predicate.Generator]
  [{:keys [gen]}] gen)

(defmethod p/to-generator [CascadingPlatform Application]
  [{:keys [source operation]}]
  (let [{:keys [op input output]} operation]
    (op-cascading op source input output)))

(defmethod p/to-generator [CascadingPlatform FilterApplication]
  [{:keys [source filter]}]
  (let [{:keys [op input]} filter]
    (filter-cascading op source input)))

(defmethod p/to-generator [CascadingPlatform ExistenceNode]
  [{:keys [source]}] source)

(defmethod p/to-generator [CascadingPlatform Join]
  [{:keys [sources join-fields type-seq options]}]
  (-> (ops/cascalog-join (map (fn [source [available type]]
                                (condp = type
                                  :inner (ops/Inner. source available)
                                  :outer (ops/Outer. source available)
                                  (ops/Existence. source available type)))
                              sources type-seq)
                         join-fields
                         (opt-seq options))
                  (ops/rename-pipe (.getName (:pipe (first sources))))))

(defmethod p/to-generator [CascadingPlatform Grouping]
  [{:keys [source aggregators grouping-fields options]}]
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

(defmethod p/to-generator [CascadingPlatform Unique]
  [{:keys [source fields options]}]
  (apply ops/unique source fields (opt-seq options)))

(defmethod p/to-generator [CascadingPlatform Merge]
  [{:keys [sources]}]
  (sum sources))

(defmethod p/to-generator [CascadingPlatform Projection]
  [{:keys [source fields]}]
  (-> source
      (ops/select* fields)
      (ops/filter-nullable-vars fields)))

(defmethod p/to-generator [CascadingPlatform Rename]
  [{:keys [source input output]}]
  (-> source
      (ops/rename* input output)
      (ops/filter-nullable-vars output)))

(defmethod p/to-generator [CascadingPlatform TailStruct]
  [item]
  (assoc (:node item) :name (-> item :options :name)))

;; ## Platform Implementation

(defn- init-pipe-name [options]
  (or (:name (:trap options))
      (u/uuid)))

(defn- init-trap-map [options]
  (if-let [trap (:trap options)]
    {(:name trap) (types/to-sink (:tap trap))}
    {}))

(defn normalize-sink-connection [sink subquery]
  (cond (fn? sink) (sink subquery)
        (instance? CascalogTap sink)
        (normalize-sink-connection (:sink sink) subquery)
        :else [sink subquery]))

(extend-protocol p/IPlatform
  CascadingPlatform
  (generator? [_ x]
    (p/platform-generator? x))

  (generator-builder [_ gen fields options]
    (-> (p/generator gen)
        (update-in [:trap-map] #(merge % (init-trap-map options)))
        (ops/rename-pipe (init-pipe-name options))
        (ops/rename* fields)
        ;; All generators if the fields aren't ungrounded discard null values
        (ops/filter-nullable-vars fields)))

  (run! [_ name bindings]
    (let [bindings (mapcat (partial apply normalize-sink-connection)
                           (partition 2 bindings))]
      (let [stats (flow/run! (apply flow/compile-flow name bindings))]
        (flow/assert-success! stats))))

  (run-to-memory! [_ name queries]
    (flow/with-stats (fn [stats]
                       (doseq [q queries :let [f (-> q :options :stats-fn)] :when f]
                         (f stats)))
      (apply flow/all-to-memory name (map p/compile-query queries)))))

;; ## Output Fields

(extend-protocol parse/IOutputFields
  Tap
  (get-out-fields [tap]
    (let [cfields (.getSourceFields tap)]
      (u/safe-assert
       (not (casc/generic-cascading-fields? cfields))
       (str "Cannot get specific out-fields from tap. Tap source fields: "
            cfields))
      (vec (seq cfields))))

  CascalogTap
  (get-out-fields [tap]
    (parse/get-out-fields (:source tap))))

;; TODO: num-out-fields should try and pluck from Tap if it doesn't
;; define output fields, rather than just throwing immediately.

(extend-protocol parse/INumOutFields
  CascalogTap
  (num-out-fields [tap]
    (parse/num-out-fields (:source tap)))

  Tap
  (num-out-fields [x]
    (count (parse/get-out-fields x))))

(extend-protocol parse/ISelectFields
  Tap
  (select-fields [tap fields]
    (-> (p/generator tap)
        (ops/select* fields)))

  CascalogTap
  (select-fields [tap fields]
    (-> (p/generator tap)
        (ops/select* fields))))
