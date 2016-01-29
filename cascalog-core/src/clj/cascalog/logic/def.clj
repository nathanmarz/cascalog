(ns cascalog.logic.def
  "This namespace contains the tools required to define custom
   Cascalog operations, instantiated with appropriate metadata."
  (:require [clojure.tools.macro :refer (name-with-attributes)]
            [cascalog.logic.fn :as s]
            [jackknife.core :refer (throw-illegal)]
            [jackknife.meta :refer (meta-update meta-conj)]))

;; ## Macros
(derive ::bufferiter ::buffer)

(defn bufferop? [op]
  (isa? (type op) ::buffer))

(defn aggregateop? [op]
  (or (isa? (type op) ::aggregate)
      (isa? (type op) ::combiner)))

(letfn [(attach [type]
          (fn [afn]
            (if-not (ifn? afn)
              (throw-illegal type " operation doesn't implement IFn: ")
              (meta-update
               (s/fn [& args]
                 (apply afn args))
               #(merge % (meta afn) {::op afn
                                     :type type})))))]
  (def mapop (attach ::map))
  (def mapcatop (attach ::mapcat))
  (def filterop (attach ::filter))
  (def aggregateop (attach ::aggregate))
  (def parallelagg (attach ::combiner))
  (def bufferop (attach ::buffer))
  (def bufferiterop (attach ::bufferiter)))

(defmacro mapfn [& body] `(mapop (s/fn ~@body)))
(defmacro mapcatfn [& body] `(mapcatop (s/fn ~@body)))
(defmacro filterfn [& body] `(filterop (s/fn ~@body)))
(defmacro aggregatefn [& body] `(aggregateop (s/fn ~@body)))
(defmacro bufferfn [& body] `(bufferop (s/fn ~@body)))
(defmacro bufferiterfn [& body] `(bufferiterop (s/fn ~@body)))

(defn- update-arglists
  "Scans the forms of a def* operation and adds an appropriate
  `:arglists` entry to the supplied `sym`'s metadata."
  [sym [form :as args]]
  (let [arglists (if (vector? form)
                   (list form)
                   (clojure.core/map clojure.core/first args))]
    (meta-conj sym {:arglists (list 'quote arglists)})))

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

;; ## Deprecated Old Timers

(defmacro defdeprecated [old new]
  `(defmacro ~old
     [sym# & body#]
     (println ~(format "Warning, %s is deprecated; use %s."
                       old
                       (resolve new)))
     `(~'~new ~sym# ~@body#)))

(defdeprecated defmapop defmapfn)
(defdeprecated deffilterop deffilterfn)
(defdeprecated defmapcatop defmapcatfn)
(defdeprecated defaggregateop defaggregatefn)
(defdeprecated defbufferop defbufferfn)
(defdeprecated defbufferiterop defbufferiterfn)

(defrecord ParallelAggregator [init-var combine-var present-var])

(defrecord ParallelBuffer
    [init-var
     combine-var
     present-var
     num-intermediate-vars-fn
     buffer-var])

;; Special node. The operation inside of here will be passed the
;; Cascalog option map and expected to return another operation.
(defrecord Prepared [op])

(defmacro defparallelbuf
  {:arglists '([name doc-string? attr-map?
                & {:keys [init-var
                          combine-var
                          extract-var
                          num-intermediate-vars-fn
                          buffer-var]}])}
  [name & body]
  (let [[name body] (name-with-attributes name body)]
    `(let [args# (hash-map ~@body)]
       (def ~name
         (ParallelBuffer. (:init-var args#)
                          (:combine-var args#)
                          (:present-var args#)
                          (:num-intermediate-vars-fn args#)
                          (:buffer-var args#))))))

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
    `(let [args# (hash-map ~@body)]
       (def ~name
         (ParallelAggregator. (:init-var args#) (:combine-var args#) (:present-var args#))))))
