(ns cascalog.logic.predicate
  (:require [clojure.string :refer (join)]
            [jackknife.core :as u]
            [cascalog.logic.vars :as v]
            [cascalog.logic.def :as d]
            [cascalog.logic.fn :refer (search-for-var)]
            [cascalog.logic.platform :as p]
            [schema.core :as s])
  (:import [clojure.lang IFn]
           [cascalog.logic.def ParallelAggregator
            ParallelBuffer Prepared]
           [jcascalog Subquery ClojureOp]))

(defprotocol IOperation
  (to-operation [_]
    "Returns a sequence of RawPredicate instances."))

(extend-protocol IOperation
  Object
  (to-operation [x] x)

  ClojureOp
  (to-operation [x] (.toVar x))

  Subquery
  (to-operation [op]
    (.getCompiledSubquery op)))

(defprotocol IRawPredicate
  (normalize [_]
    "Returns a sequence of RawPredicate instances."))

;; Raw Predicate type.

(defrecord RawPredicate [op input output]
  IRawPredicate
  (normalize [p] [p]))

;; Output of the subquery, the predicates it contains and the options
;; in the subquery.

(defrecord RawSubquery [fields predicates options])

;; Printing Methods
;;
;; The following methods allow a predicate to print properly.

(s/defmethod print-method RawPredicate
  [{:keys [op input output]} :- RawPredicate
   writer :- java.io.Writer]
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

(s/defmethod print-method RawSubquery
  [{:keys [fields predicates options] :as s} :- RawSubquery
   writer :- java.io.Writer]
  (binding [*out* writer]
    (println "(<-" (vec fields))
    (doseq [[opt v] options :when v]
      (print "    ")
      (println (format "(%s %s)" opt v)))
    (doseq [pred predicates]
      (print "    ")
      (print-method pred writer))
    (println "    )")))

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

  clojure.lang.Var
  (filter? [v] (fn? @v))

  clojure.lang.MultiFn
  (filter? [_] true))

(defprotocol INode
  (node? [_] "Returns true if the object is a node, false otherwise."))

(extend-protocol INode
  Object (node? [_] false))

(defmacro defnode [sym fields & more]
  {:pre [(not (contains? fields 'identifier))]}
  (let [ns-part   (namespace-munge *ns*)
        classname (symbol (str ns-part "." sym))
        docstring (str "Positional factory function for class " classname ".")]
    `(do (defrecord ~sym [~@(cons 'identifier fields)]
           INode
           (node? [_] true)
           ~@more)
         (defn ~(symbol (str '-> sym))
           ~docstring
           [~@fields]
           (new ~classname (u/uuid) ~@fields))
         (defn ~(symbol (str 'map-> sym))
           ~(str "Factory function for class "
                 classname
                 ", taking a map of keywords to field values.")
           ([m#] (~(symbol (str classname "/create"))
                  (-> {:identifier (u/uuid)}
                      (merge m#))))))))

;; Leaves of the tree:
(defnode Generator [gen fields])

;; GeneratorSets can't be unground, ever.
(defrecord GeneratorSet [generator join-set-var])

(defrecord Operation [op input output])

;; filters can be applied to Generator or GeneratorSet.
(defrecord FilterOperation [op input])

(defrecord Aggregator [op input output])

(defn can-generate? [op]
  (or (node? op)
      (p/generator? p/*platform* op)
      (instance? GeneratorSet op)))

(defn generator-node
  "Converts the supplied generator into the proper type of node."
  [gen input output options]
  {:pre [(empty? input)]}
  (if (instance? GeneratorSet gen)
    (let [{:keys [generator] :as op} gen]
      (assert ((some-fn node? (partial p/generator? p/*platform*)) generator)
              (str "Only Nodes or Generators allowed: " generator))
      (assoc op :generator (generator-node generator input output options)))
    (->Generator (p/generator-builder p/*platform* gen output options)
                 output)))

;; The following multimethod converts operations (in the first
;; position of a parsed cascalog predicate) to nodes in the graph.

(defmulti to-predicate
  (fn [op input output]
    (type op)))

(defmethod to-predicate :default
  [op _ _]
  (u/throw-illegal (str op " is an invalid predicate.")))

;; ## Operations

(defmethod to-predicate IFn
  [op input output]
  (if-let [output (not-empty output)]
    (Operation. (d/mapop op) input output)
    (FilterOperation. (d/filterop op) input)))

(defmethod to-predicate ::d/filter
  [op input output]
  (if-let [output (not-empty output)]
    (Operation. (d/mapop (-> op meta ::d/op)) input output)
    (FilterOperation. op input)))

(defmethod to-predicate ::d/map
  [op input output]
  (def cake op)
  (if-let [output (not-empty output)]
    (Operation. op input output)
    (FilterOperation. (d/filterop (-> op meta ::d/op)) input)))

(defmethod to-predicate ::d/mapcat
  [op input output]
  (Operation. op input output))

;; ## Aggregators

(defmethod to-predicate ::d/buffer
  [op input output]
  (Aggregator. op input output))

(defmethod to-predicate ::d/bufferiter
  [op input output]
  (Aggregator. op input output))

(defmethod to-predicate ::d/aggregate
  [op input output]
  (Aggregator. op input output))

(defmethod to-predicate ::d/combiner
  [op input output]
  (Aggregator. op input output))

(defmethod to-predicate ParallelAggregator
  [op input output]
  (Aggregator. op input output))

(defmethod to-predicate ParallelBuffer
  [op input output]
  (Aggregator. op input output))

(defn build-predicate
  "Accepts an option map and a raw predicate and returns a node in the
  Cascalog graph."
  [options {:keys [op input output] :as pred}]
  (cond (or (p/generator? p/*platform* op)
            (instance? GeneratorSet op))
        (generator-node op input output options)

        (instance? Prepared op)
        (build-predicate options
                         (assoc pred :op ((:op op) options)))

        :else (to-predicate op input output)))
