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
  (let [[name body] (name-with-attributes name body)
        name        (-> name
                        (update-arglists body)
                        (u/meta-conj {:pred-type type-kwd}))
        params (:params (meta name))
        name   (u/meta-update name #(dissoc % :params))
        runner-name (symbol (str name "__"))
        runner-var `(var ~runner-name)]
    `(do (defn ~runner-name
           ~(assoc (meta name)
              :no-doc true
              :skip-wiki true)
           ~@(if params
               `[~params (serfn/fn ~@body)]
               body))
         ~(if params
            `(defn ~name
               {:arglists '(~params)}
               [& args#]
               (with-meta
                 (~delegate (apply ~runner-var args#))
                 ~(meta name)))
            `(def ~name
               (with-meta
                 ~(list delegate runner-var)
                 ~(meta name)))))))

(defmacro defdefop
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
