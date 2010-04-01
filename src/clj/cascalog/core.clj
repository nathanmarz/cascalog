(ns cascalog.core
  (:use [cascalog vars util])
  (:require [cascalog [workflow :as w] [predicate :as p]])
  (:import [cascading.tap Tap])
  (:import [cascading.tuple Fields]))

;; TODO:
;; 
;; 4. Enforce !! rules -> only allowed in generators or output of operations, ungrounds whatever it's in

;; TODO: make it possible to create ungrounded rules that take in input vars (for composition)
;; i.e. (<- [?a ?b :> ])
(defn build-rule [out-vars raw-predicates]
  (let [[out-vars vmap] (uniquify-vars out-vars {})
        update-fn       (fn [[preds vmap] [op opvar vars]]
                          (let [[newvars vmap] (uniquify-vars vars vmap)]
                            [(conj preds [op opvar newvars]) vmap] ))
        [raw-predicates _] (reduce update-fn [[] vmap] raw-predicates)
        predicates         (map (partial apply p/build-predicate) raw-predicates) ]
        predicates
    ))

(defn- mk-raw-predicate [pred]
  (let [[op-sym & vars] pred
        str-vars (vars2str vars)]
    [op-sym (try-resolve op-sym) str-vars]))

(defmacro <-
  "Constructs a rule from a list of predicates"
  [outvars & predicates]
  (let [predicate-builders (vec (map mk-raw-predicate predicates))
        outvars-str (vars2str outvars)]
        `(cascalog.core/build-rule ~outvars-str ~predicate-builders)))

