(ns cascalog.fluent.def
  (:require [cascalog.fluent.operations :as ops]
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
        runner-name (symbol (str name "__"))
        runner-var `(var ~runner-name)]
    `(do (defn ~runner-name
           ~(assoc (meta name)
              :no-doc true
              :skip-wiki true)
           ~@body)
         (def ~name
           (with-meta
             ~(list delegate runner-var)
             ~(meta name))))))

(defmacro defdefop
  [sym & body]
  (let [[sym [delegate type-kwd]] (name-with-attributes sym body)]
    `(defmacro ~sym
       {:arglists '~'([name doc-string? attr-map? [fn-args*] body])}
       [sym# & body#]
       (defop-body ~delegate ~type-kwd sym# body#))))

;;
(defdefop defmapop #'ops/mapop :map)
(defdefop defmapcatop #'ops/mapcatop :mapcat)
(defdefop deffilterop #'ops/filterop :filter)
