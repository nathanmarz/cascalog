(ns cascalog.fluent.def
  (:require [clojure.tools.macro :refer (name-with-attributes)]
            [cascalog.util :as u]
            [cascalog.fluent.operations :as ops]
            [cascalog.fluent.fn :as s]
            [jackknife.core :refer (throw-illegal)]))

;; ## Macros

(defn prepared
  "Marks the supplied operation as needing to be prepared by
  Cascading. The supplied op should take two arguments and return
  another IFn for use by Cascading."
  [afn]
  (u/meta-update afn #(merge % {::prepared true})))

;; TODO: This runs into trouble if you want to return a map to use as
;; a function. Make an interface that we can reify to make a prepared
;; operation if we want a cleanup.

(defmacro prepfn
  "Defines a prepared operation. Pass in an argument vector of two
  items and return either a function or a Map with two
  keywords; :operate and :cleanup"
  [args & body] {:pre [(= 2 (count args))]}
  `(prepared (s/fn ~args ~@body)))

(defn prepared?
  "Returns true if the supplied operation needs to be supplied the
  FlowProcess and operation call by Cascading on instantiation, false
  otherwise."
  [op]
  (= true (-> op meta ::prepared)))

(derive ::map ::operation)
(derive ::mapcat ::operation)

(derive ::bufferiter ::buffer)

(defn buffer? [op]
  (isa? (type op) ::buffer))

(defn aggregator? [op]
  (or (isa? (type op) ::aggregate)
      (isa? (type op) ::combiner)))

(letfn [(attach [builder type]
          (fn [afn]
            (if-not (ifn? afn)
              (throw-illegal type " operation doesn't implement IFn: ")
              (with-meta
                (s/fn [& args]
                  (apply afn args))
                (merge (meta afn) {::op afn
                                   ::op-builder builder
                                   :type type})))))]
  (def mapop* (attach ops/map* ::map))
  (def mapcatop* (attach ops/mapcat* ::mapcat))
  (def filterop* (attach ops/filter* ::filter))
  (def aggregateop* (attach ops/agg ::aggregate))
  (def parallelagg* (attach ops/parallel-agg ::combiner))
  (def bufferop* (attach ops/buffer ::buffer))
  (def bufferiterop* (attach ops/bufferiter ::bufferiter)))

(defmacro mapfn [& body] `(mapop* (s/fn ~@body)))
(defmacro mapcatfn [& body] `(mapcatop* (s/fn ~@body)))
(defmacro filterfn [& body] `(filterop* (s/fn ~@body)))
(defmacro aggregatefn [& body] `(aggregateop* (s/fn ~@body)))
(defmacro bufferfn [& body] `(bufferop* (s/fn ~@body)))
(defmacro bufferiterfn [& body] `(bufferiterop* (s/fn ~@body)))

(defn- update-arglists
    "Scans the forms of a def* operation and adds an appropriate
  `:arglists` entry to the supplied `sym`'s metadata."
    [sym [form :as args]]
    (let [arglists (if (vector? form)
                     (list form)
                     (clojure.core/map clojure.core/first args))]
      (u/meta-conj sym {:arglists (list 'quote arglists)})))

(defn defhelper [name op-sym body]
  (let [[name body] (name-with-attributes name body)
        name        (update-arglists name body)]
    `(def ~name (~op-sym ~@body))))

(defmacro defdefop
    "Helper macro to define the def*op macros."
    [sym & body]
    (let [[sym [delegate]] (name-with-attributes sym body)]
      `(defmacro ~sym
         {:arglists '~'([name doc-string? attr-map? [fn-args*] body])}
         [sym# & body#]
         (defhelper sym# ~delegate body#))))

(defdefop defmapfn
  "Defines a map operation."
  `mapfn)

(defdefop defmapcatfn
  "Defines a mapcat operation."
  `mapcatfn)

(defdefop deffilterfn
  "Defines a filtering operation."
  `filterfn)

(defdefop defaggregatefn
  "Defines a filtering operation."
  `aggregatefn)

(defdefop defbufferfn
  "Defines a filtering operation."
  `bufferfn)

(defdefop defbufferiterfn
  "Defines a filtering operation."
  `bufferiterfn)

(comment
  "REMAINING MACROS"
  (defmacro defmultibufferop [name & body] (defhelper name `multibufferop body)))

;; ## Deprecated Old Timers

(defmacro defdeprecated [old new]
  `(defmacro ~old
     [sym# & body#]
     (println ~(format "Warning, %s is deprecated; use %s."
                       (resolve old)
                       (resolve new)))
     `(defmapcatfn ~sym# ~@body#)))

(defdeprecated defmapop defmapfn)
(defdeprecated deffilterop deffilterfn)
(defdeprecated defmapcatop defmapcatfn)
(defdeprecated defaggregateop defaggregatefn)
(defdeprecated defbufferop defbufferfn)
(defdeprecated defbufferiterop defbufferiterfn)

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
  {:arglists '([name doc-string? attr-map?
                & {:keys [init-var combine-var present-var]}])}
  [name & body]
  (let [[name body] (name-with-attributes name body)]
    `(def ~name
       (map->ParallelAggregator (hash-map ~@body)))))

;; ## Runner
;;
;; exec* can be used to run an operation directly within the fluent
;; API.
;;
;; TODO: Move back to normal API.

(defn exec*
  "Accepts an operation and applies it to the given flow."
  [flow op & args]
  (let [{builder ::op-builder backing-op ::op} (meta op)]
    (apply (or builder ops/map*)
           flow
           (or backing-op op)
           args)))

(defn build-agg
  "Accepts an aggregator and applies it to the given flow."
  [op in-fields out-fields]
  (let [{builder ::op-builder backing-op ::op} (meta op)]
    (assert (and builder backing-op) "You have to supply an aggregator.")
    (builder backing-op in-fields out-fields)))
