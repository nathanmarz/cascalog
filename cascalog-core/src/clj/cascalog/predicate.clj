(ns cascalog.predicate
  (:use [jackknife.core :only (throw-illegal)])
  (:require [jackknife.core :as u]
            [clojure.zip :as zip]
            [cascalog.vars :as v]
            [cascalog.fluent.def :as d]
            [cascalog.fluent.cascading :as casc]
            [cascalog.fluent.operations :as ops]
            [cascalog.fluent.types :as types]
            [cascalog.fluent.flow :as f])
  (:import [clojure.lang IFn]
           [cascalog.fluent.def ParallelAggregator]
           [cascalog.fluent.types IGenerator]
           [cascading.pipe Each Every]
           [cascading.tap Tap]
           [cascading.operation Filter]
           [jcascalog Subquery ClojureOp]
           [cascalog.aggregator CombinerSpec]
           [cascalog CascalogFunction
            CascalogFunctionExecutor CascadingFilterToFunction
            CascalogBuffer CascalogBufferExecutor CascalogAggregator
            CascalogAggregatorExecutor ClojureParallelAgg ParallelAgg]))

;; ## ICouldFilter

(defprotocol ICouldFilter
  "This protocol exists so that Cascalog can decide, if no input or
   output signifier exists, if the function takes inputs or outputs by
   default."
  (filter? [_]
    "Returns true if the object could filter, false otherwise."))

(extend-protocol ICouldFilter
  Object
  (filter? [_] false)

  clojure.lang.Fn
  (filter? [_] true)

  clojure.lang.MultiFn
  (filter? [_] true)

  Filter
  (filter? [_] true))

;; Leaves of the tree:
(defrecord Generator [source-map trap-map pipe fields])

;; GeneratorSets can't be unground, ever.
(defrecord GeneratorSet [generator join-set-var])

;; TODO: Consider moving the assembly out and keeping the operation by
;; itself. The assembly should probably be part of the Hadoop planner.
(defrecord Operation [assembly input output])

;; filters can be applied to Generator or GeneratorSet.
(defrecord FilterOperation [assembly input])

(defrecord Aggregator [op input output])

(defn validate-generator-set!
  "GeneratorSets can't be unground, ever."
  [input output]
  (when (not-empty input)
    (when (> (count output) 1)
      (throw-illegal "Only one output variable allowed in a generator-as-set."))
    (when-let [unground (not-empty (filter v/unground-var? (concat input output)))]
      (throw-illegal (str "Can't use unground vars in generators-as-sets. "
                          (vec unground)
                          " violate(s) the rules.\n\n")))))

(defn generator-node
  "Converts the supplied generator into the proper type of node."
  [gen input output]
  {:pre [(types/generator? gen)]}
  (let [{:keys [pipe source-map trap-map]} (-> (types/generator gen)
                                               (ops/rename* output))
        generator (->Generator source-map trap-map pipe output)]
    (if-let [[join-set-var] (not-empty input)]
      (do (validate-generator-set! input output)
          (->GeneratorSet generator join-set-var))
      generator)))

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

;; The following multimethod converts operations (in the first
;; position of a parsed cascalog predicate) to nodes in the graph.

(defmulti to-predicate
  (fn [op input output]
    (type op)))

(defmethod to-predicate :default
  [op _ _]
  (u/throw-illegal (str op " is an invalid predicate.")))

;; ## Operations

(defmethod to-predicate :cascalog.fluent.def/filter
  [op input _]
  (->FilterOperation (filter-assem [in] (ops/filter* op in))
                     input))

(defmethod to-predicate :cascalog.fluent.def/map
  [op input output]
  (->Operation (assem [in out] (ops/map* op in out))
               input
               output))

(defmethod to-predicate :cascalog.fluent.def/mapcat
  [op input output]
  (->Operation (assem [in out] (ops/mapcat* op in out))
               input
               output))

(defmethod to-predicate Subquery
  [op input output]
  (to-predicate (.getCompiledSubquery op) input output))

(defmethod to-predicate ClojureOp
  [op input output]
  (to-predicate (.toVar op) input output))

(defmethod to-predicate IFn
  [op input output]
  (if-let [output (not-empty output)]
    (to-predicate (d/mapop* op) input output)
    (to-predicate (d/filterop* op) input output)))

(defmethod to-predicate Filter
  [op input output]
  (u/safe-assert (#{0 1} (count output)) "Must emit 0 or 1 fields from filter")
  (if (empty? output)
    (->FilterOperation (filter-assem [in] (ops/add-op #(Each. % in op)))
                       input)
    (->Operation (assem
                  [in out]
                  (ops/each #(CascadingFilterToFunction. (first %) op) in out))
                 input
                 output)))

(defmethod to-predicate CascalogFunction
  [op input output]
  (->Operation (ops/assembly [in out]
                             (ops/each #(CascalogFunctionExecutor. % op) in out))
               input
               output))

;; ## Aggregators

(defmethod to-predicate :cascalog.fluent.def/buffer
  [op input output]
  (->Aggregator (fn [in out]
                  (ops/buffer op in out))
                input output))

(defmethod to-predicate :cascalog.fluent.def/bufferiter
  [op input output]
  (->Aggregator (fn [in out] (ops/bufferiter op in out))
                input
                output))

(defmethod to-predicate :cascalog.fluent.def/aggregate
  [op input output]
  (->Aggregator (fn [in out] (ops/agg op in out))
                input
                output))

(defmethod to-predicate :cascalog.fluent.def/combiner
  [op input output]
  (->Aggregator (fn [in out] (ops/parallel-agg op in out))
                input
                output))

(defmethod to-predicate ParallelAggregator
  [op input output]
  (->Aggregator (fn [in out]
                  (ops/parallel-agg (:combine-var op) in out))
                input output))

(defmethod to-predicate CascalogBuffer
  [op input output]
  (->Aggregator (fn [in out]
                  (reify ops/IBuffer
                    (add-buffer [_ pipe]
                      (Every. pipe in
                              (CascalogBufferExecutor. (casc/fields out) op)))))
                input
                output))

;; TODO: jcascalog ParallelAgg.

(defmethod to-predicate CascalogAggregator
  [op input output]
  (->Aggregator (fn [in out]
                  (reify ops/IAggregator
                    (add-aggregator [_ pipe]
                      (Every. pipe in
                              (CascalogAggregatorExecutor. (casc/fields out) op)))))

                input
                output))

(defn build-predicate
  "Accepts a raw predicate and returns a node in the Cascalog graph."
  [{:keys [op input output]}]
  (if (types/generator? op)
    (generator-node op input output)
    (to-predicate op input output)))

(comment
  "TODO: Convert to test."
  (let [gen (-> (types/generator [1 2 3 4])
                (ops/rename* "?x"))
        pred (to-predicate * ["?a" "?a"] ["?b"])]
    (fact
      (f/to-memory
       ((:assembly pred) gen ["?x" "?x"] "?z"))
      => [[1 1] [2 4] [3 9] [4 16]])))
