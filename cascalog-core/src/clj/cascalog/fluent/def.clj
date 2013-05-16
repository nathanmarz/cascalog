(ns cascalog.fluent.def
  (:require [cascalog.fluent.operations :as ops]
            [cascalog.fluent.fn :as serfn]
            [clojure.tools.macro :refer (name-with-attributes)]
            [cascalog.util :as u]))

;; ## Macros

(defn- update-arglists
  "Scans the forms of a def* operation and adds an appropriate
  `:arglists` entry to the supplied `sym`'s metadata."
  [sym [form :as args]]
  (let [arglists (if (vector? form)
                   (list form)
                   (clojure.core/map clojure.core/first args))]
    (u/meta-conj sym {:arglists (list 'quote arglists)})))

(defn defop-body
  [delegate type-kwd name body]
  (let [runner-name (symbol (str name "__"))
        runner-var `(var ~runner-name)
        [name body] (name-with-attributes name body)
        name        (-> name
                        (update-arglists body)
                        (u/meta-conj {:pred-type type-kwd
                                      :runner runner-var}))
        params (:params (meta name))
        name (u/meta-update name #(dissoc % :params))
        runner-meta (-> (meta name)
                        (dissoc :runner)
                        (assoc :no-doc true
                               :skip-wiki true))]
    `(do (defn ~runner-name
           ~runner-meta
           ~@(if params
               `[~params (serfn/fn ~@body)]
               body))
         (def ~name
           (with-meta
             ~(if params
                `(fn [& args#]
                   (with-meta
                     (~delegate (apply ~runner-var args#))
                     ~(meta name)))
                (list delegate runner-var))
             ~(meta name))))))

(defn runner
  "Returns the backing operation for a function defined with one of
  the def*op macros."
  [op]
  (-> op meta :runner))

(defmacro defdefop
  "Helper macro to define the def*op macros."
  [sym & body]
  (let [[sym [delegate type-kwd]] (name-with-attributes sym body)]
    `(defmacro ~sym
       {:arglists '~'([name doc-string? attr-map? [fn-args*] body])}
       [sym# & body#]
       (defop-body ~delegate ~type-kwd sym# body#))))

(defdefop defmapop
  "Defines a map operation."
  #'ops/mapop :map)

(defdefop defmapcatop
  "Defines an operation from input to some sequence of outputs."
  #'ops/mapcatop :mapcat)

(defdefop deffilterop
  "Defines an operation with only input fields to be called as a
  filter."
  #'ops/filterop :filter)
