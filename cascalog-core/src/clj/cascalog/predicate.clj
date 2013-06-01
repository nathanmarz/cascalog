(ns cascalog.predicate
  (:use [cascalog.util :only (uuid multifn? substitute-if search-for-var any-list?)]
        [jackknife.seq :only (transpose)]
        [clojure.tools.macro :only (name-with-attributes)])
  (:require [jackknife.core :as u]
            [clojure.zip :as zip]
            [cascalog.vars :as v]
            [cascalog.fluent.workflow :as w]
            [cascalog.fluent.operations :as ops]
            [cascalog.fluent.types :as types])
  (:import [cascalog.fluent.types IGenerator]
           [cascading.tap Tap]
           [cascading.operation Filter]
           [cascading.tuple Fields]
           [clojure.lang IFn]
           [jcascalog PredicateMacro Subquery ClojureOp PredicateMacroTemplate]
           [cascalog.aggregator CombinerSpec]
           [cascalog ClojureParallelAggregator ClojureBuffer
            ClojureBufferCombiner CascalogFunction
            CascalogFunctionExecutor CascadingFilterToFunction
            CascalogBuffer CascalogBufferExecutor CascalogAggregator
            CascalogAggregatorExecutor ClojureParallelAgg ParallelAgg]))

(defrecord ParallelAggregator [init-var combine-var present-var])

(defmacro defparallelagg
  "Binds an efficient aggregator to the supplied symbol. A parallel
  aggregator processes each tuple through an initializer function,
  then combines the results each tuple's initialization until one
  result is achieved. `defparallelagg` accepts two keyword arguments:

  :init-var -- A var bound to a fn that accepts raw tuples and returns
  an intermediate result; #'one, for example.

  :combine-var -- a var bound to a fn that both accepts and returns
  intermediate results.

  For example,

  (defparallelagg sum
  :init-var #'identity
  :combine-var #'+)

  Used as

  (sum ?x :> ?y)"
  {:arglists '([name doc-string? attr-map? & {:keys [init-var combine-var]}])}
  [name & body]
  (let [[name body] (name-with-attributes name body)]
    `(def ~name
       (map->ParallelAggregator (hash-map ~@body)))))

;; Leaves of the tree:
(defrecord Generator [ground? source-map trap-map pipe fields])

;; GeneratorSets can't be unground, ever.
(defrecord GeneratorSet [join-set-var source-map trap-map pipe fields])

;; map, mapcat operations:
(defrecord Operation [op infields outfields])

;; filters can be applied to Generator or GeneratorSet.
(defrecord FilterOperation [op infields])

;; Sort by filter vs operation, then try and apply all as a fixed
;; point. Write the fixed point application in terms of the tail.

;; TODO: Implement IGenerator here.
(defrecord TailStruct [root operations drift-map fields children])

;; ## Operation Application

(defprotocol IApplyToTail
  (accept? [this tail]
    "Returns true if this op can be applied to the current tail")

  (apply-to-tail [this tail]
    "Accepts a tail and performs some modification on that tail,
    returning a new tail."))


;; I think this is duplicated; we can merge this idea together with
;; the types in the fluent API.
(defrecord Aggregator [assembly input output])

;; Currently predicate macros are expanded in build-rule, in
;; rules.clj. By the time we get here, let's assume that everything
;; has been properly parsed and expanded.

(defn predicate-dispatcher [pred]
  (let [op  (:op pred)
        ret (cond (types/generator? op)             ::generator
                  (instance? Filter op)             ::cascading-filter
                  (instance? CascalogFunction op)   ::cascalog-function
                  (instance? CascalogBuffer op)     ::cascalog-buffer
                  (instance? CascalogAggregator op) ::cascalog-aggregator
                  (instance? ParallelAgg op)        ::java-parallel-agg

                  ;; This dispatches generator-filter, generator,
                  ;; aggregator, operation.
                  (map? op)                         (:type op)
                  (or (vector? op) (any-list? op))  ::data-structure
                  (:pred-type (meta op))            (:pred-type (meta op))
                  (instance? IFn op)                ::vanilla-function
                  :else (u/throw-illegal (str op " is an invalid predicate.")))]
    (if (= ret :bufferiter) :buffer ret)))

(defn predicate-macro? [p]
  (or (var? p)
      (instance? PredicateMacro p)
      (instance? PredicateMacroTemplate p)
      (instance? Subquery p)
      (instance? ClojureOp p)
      (and (map? p) (= :predicate-macro (:type p)))))

(defn- simpleop-build-predicate
  [op infields outfields _]
  (predicate operation
             (op infields :fn> outfields :> Fields/ALL)
             infields
             outfields
             false))

(defn- simpleagg-build-predicate
  [buffer? op infields outfields _]
  (predicate aggregator
             buffer?
             nil
             identity
             (op infields :fn> outfields :> Fields/ALL)
             identity
             infields
             outfields))

(defmulti default-var predicate-dispatcher :default :>)
(defmethod default-var ::vanilla-function [& _] :<)
(defmethod default-var :filter [& _] :<)
(defmethod default-var ::cascading-filter [& _] :<)

(defmulti build-predicate predicate-dispatcher)

;; TODO: Validation on the input and output here.

(defmethod build-predicate ::generator
  [{:keys [op input output options] :as pred}]
  (let [{:keys [pipe source-map trap-map]} (-> (types/generator op)
                                               (ops/rename* output))]
    (->Generator {:ground? (v/fully-ground? output)
                  :source-map source-map
                  :trap-map trap-map
                  :pipe pipe
                  :output output})))

(defmethod build-predicate ::java-parallel-agg
  [java-pagg infields outfields _]
  (let [cascading-agg (ClojureParallelAggregator. (w/fields outfields)
                                                  java-pagg
                                                  (count infields))
        serial-assem (if (empty? infields)
                       (w/raw-every cascading-agg Fields/ALL)
                       (w/raw-every (w/fields infields)
                                    cascading-agg
                                    Fields/ALL))]
    (predicate aggregator
               false
               java-pagg
               identity
               serial-assem
               identity
               infields
               outfields)))

(defmethod build-predicate ::parallel-aggregator
  [pagg infields outfields options]
  (let [init-spec (ops/fn-spec (:init-var pagg))
        combine-spec (ops/fn-spec (:combine-var pagg))
        java-pagg (ClojureParallelAgg. (-> (CombinerSpec. combine-spec)
                                           (.setPrepareFn init-spec)))]
    (build-predicate java-pagg infields outfields options)
    ))

(defmethod build-predicate ::vanilla-function
  [afn infields outfields _]
  (let [[func-fields out-selector] (if (not-empty outfields)
                                     [outfields Fields/ALL]
                                     [nil nil])
        assembly (w/filter afn infields :fn> func-fields :> out-selector)]
    (predicate operation assembly infields outfields false)))

(defmethod build-predicate :map [& args]
  (apply simpleop-build-predicate args))

(defmethod build-predicate :mapcat [& args]
  (apply simpleop-build-predicate args))

(defmethod build-predicate :aggregate [& args]
  (apply simpleagg-build-predicate false args))

(defmethod build-predicate :buffer [& args]
  (apply simpleagg-build-predicate true args))

(defmethod build-predicate :filter
  [op infields outfields _]
  (let [[func-fields out-selector] (if (not-empty outfields)
                                     [outfields Fields/ALL]
                                     [nil nil])
        assembly (op infields :fn> func-fields :> out-selector)]
    (predicate operation
               assembly
               infields
               outfields
               false)))

(defmethod build-predicate ::cascalog-function
  [op infields outfields _]
  (predicate operation
             (w/raw-each (w/fields infields)
                         (CascalogFunctionExecutor. (w/fields outfields) op)
                         Fields/ALL)
             infields
             outfields
             false))

(defmethod build-predicate ::cascading-filter
  [op infields outfields _]
  (u/safe-assert (#{0 1} (count outfields))
                 "Must emit 0 or 1 fields from filter")
  (let [c-infields (w/fields infields)
        assem (if (empty? outfields)
                (w/raw-each c-infields op)
                (w/raw-each c-infields
                            (CascadingFilterToFunction. (first outfields) op)
                            Fields/ALL))]
    (predicate operation assem infields outfields false)))

(defmethod build-predicate ::cascalog-buffer
  [op infields outfields options]
  (predicate aggregator
             true
             nil
             identity
             (w/raw-every (w/fields infields)
                          (CascalogBufferExecutor. (w/fields outfields) op)
                          Fields/ALL)
             identity
             infields
             outfields))

(defmethod build-predicate ::cascalog-aggregator
  [op infields outfields _]
  (predicate aggregator
             false
             nil
             identity
             (w/raw-every (w/fields infields)
                          (CascalogAggregatorExecutor. (w/fields outfields) op)
                          Fields/ALL)
             identity
             infields
             outfields))

(defmethod build-predicate :generator-filter
  [op infields outfields options]
  (-> (build-predicate (:generator op)
                       infields
                       outfields
                       options)
      (assoc :join-set-var (:outvar op))))

(defmethod build-predicate :outconstant-equal
  [_ infields outfields options]
  (-> (build-predicate = infields outfields options)
      (assoc :allow-on-genfilter? true)))
