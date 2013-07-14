(ns cascalog.logic.predicate
  "TODO: We need to remove all of the Cascading implementations from
   here. The extensions to to-predicate."
  (:require [jackknife.core :as u]
            [clojure.zip :as zip]
            [cascalog.logic.vars :as v]
            [cascalog.logic.def :as d]
            [cascalog.cascading.util :as casc]
            [cascalog.cascading.operations :as ops]
            [cascalog.cascading.types :as types]
            [cascalog.cascading.flow :as f])
  (:import [clojure.lang IFn]
           [cascalog.logic.def ParallelAggregator Prepared]
           [cascalog.cascading.types IGenerator]
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
  (filter? [_] true))

;; Leaves of the tree:
(defrecord Generator [gen fields])

;; GeneratorSets can't be unground, ever.
(defrecord GeneratorSet [generator join-set-var])

(defrecord Operation [op input output])

;; filters can be applied to Generator or GeneratorSet.
(defrecord FilterOperation [op input])

(defrecord Aggregator [op input output])

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

(defn sanitize-output
  "If the generator has duplicate output fields, this function
  generates duplicates and applies the proper equality operations."
  [gen output]
  (let [[_ cleaned] (ops/replace-dups output)]
    [cleaned (reduce (fn [acc [old new]]
                       (if (= old new)
                         acc
                         (-> acc (ops/filter* = [old new]))))
                     (-> (types/generator gen)
                         (ops/rename* cleaned)
                         (ops/filter-nullable-vars cleaned))
                     (map vector output cleaned))]))

(defn generator-node
  "Converts the supplied generator into the proper type of node."
  [gen input output]
  {:pre [(types/generator? gen)]}
  (if-not (empty? input)
    (do (validate-generator-set! input output)
        (-> (generator-node gen [] input)
            (->GeneratorSet (first output))))
    (let [[cleaned gen] (sanitize-output gen output)
          {:keys [pipe source-map trap-map]} gen]
      (->Generator gen cleaned))))

;; The following multimethod converts operations (in the first
;; position of a parsed cascalog predicate) to nodes in the graph.

(defmulti to-predicate
  (fn [op input output]
    (type op)))

(defmethod to-predicate :default
  [op _ _]
  (u/throw-illegal (str op " is an invalid predicate.")))

;; ## Operations

(defmethod to-predicate Subquery
  [op input output]
  (to-predicate (.getCompiledSubquery op) input output))

(defmethod to-predicate ClojureOp
  [op input output]
  (to-predicate (.toVar op) input output))

(defmethod to-predicate IFn
  [op input output]
  (if-let [output (not-empty output)]
    (->Operation (d/mapop* op) input output)
    (->FilterOperation (d/filterop* op) input)))

;; TODO: Get rid of this whole mess.
(defmethod to-predicate :cascalog.logic.def/filter
  [op input output]
  (->FilterOperation op input))

(defmethod to-predicate :cascalog.logic.def/map
  [op input output]
  (->Operation op input output))

(defmethod to-predicate :cascalog.logic.def/mapcat
  [op input output]
  (->Operation op input output))

(defmethod to-predicate Filter
  [op input output]
  (u/safe-assert (#{0 1} (count output)) "Must emit 0 or 1 fields from filter")
  (if (empty? output)
    (->FilterOperation op input)
    (->Operation op input output)))

(defmethod to-predicate CascalogFunction
  [op input output]
  (->Operation op input output))

;; ## Aggregators

(defmethod to-predicate :cascalog.logic.def/buffer
  [op input output]
  (->Aggregator (fn [in out]
                  (ops/buffer op in out))
                input output))

(defmethod to-predicate :cascalog.logic.def/bufferiter
  [op input output]
  (->Aggregator (fn [in out] (ops/bufferiter op in out))
                input
                output))

(defmethod to-predicate :cascalog.logic.def/aggregate
  [op input output]
  (->Aggregator (fn [in out] (ops/agg op in out))
                input
                output))

(defmethod to-predicate :cascalog.logic.def/combiner
  [op input output]
  (->Aggregator (fn [in out] (ops/parallel-agg op in out))
                input
                output))

(defmethod to-predicate ParallelAggregator
  [op input output]
  (->Aggregator (fn [in out]
                  (ops/parallel-agg (:combine-var op) in out
                                    :init-var (:init-var op)
                                    :present-var (:present-var op)))
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
  "Accepts an option map and a raw predicate and returns a node in the
  Cascalog graph."
  [options {:keys [op input output] :as pred}]
  (cond (types/generator? op)   (generator-node op input output)
        (instance? Prepared op) (build-predicate options
                                                 (assoc pred :op ((:op op) options)))
        :else                   (to-predicate op input output)))

(comment
  "TODO: Convert to test."
  (let [gen (-> (types/generator [1 2 3 4])
                (ops/rename* "?x"))
        pred (to-predicate * ["?a" "?a"] ["?b"])]
    (fact
     (f/to-memory
      ((:op pred) gen ["?x" "?x"] "?z"))
     => [[1 1] [2 4] [3 9] [4 16]])))
