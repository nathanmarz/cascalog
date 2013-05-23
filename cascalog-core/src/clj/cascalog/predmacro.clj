(ns cascalog.predmacro
  (:require [cascalog.vars :as v]
            [cascalog.util :as u]
            [cascalog.predicate :as p]
            [cascalog.parse :as parse]
            [clojure.set :refer (intersection)]
            [clojure.walk :refer (postwalk)]
            [jackknife.core :refer (throw-runtime)])
  (:import [jcascalog Predicate Subquery PredicateMacro ClojureOp
            PredicateMacroTemplate]))

;; ## Predicate Macro Building Functions

(defn- new-var-name!
  [replacements v]
  (let [new-name (if (contains? @replacements v)
                   (@replacements v)
                   (v/uniquify-var v))]
    (swap! replacements assoc v new-name)
    new-name))

(defn- pred-macro-updater [[replacements ret] [op vars]]
  (let [vars (vec vars) ;; in case it's a java data structure
        newvars (postwalk #(if (v/cascalog-var? %)
                             (new-var-name! replacements %)
                             %)
                          vars)]
    [replacements (conj ret [op newvars])]))

(defn- build-predicate-macro-fn
  [invars-decl outvars-decl raw-predicates]
  (when (seq (intersection (set invars-decl)
                           (set outvars-decl)))
    (throw-runtime
     (format
      "Cannot declare the same var as an input and output to predicate macro: %s %s"
      invars-decl
      outvars-decl)))
  (fn [invars outvars]
    (let [outvars (if (and (empty? outvars)
                           (sequential? outvars-decl)
                           (= 1 (count outvars-decl)))
                    [true]
                    outvars)
          ;; kind of a hack, simulate using pred macros like filters
          replacements (atom (u/mk-destructured-seq-map invars-decl
                                                        invars
                                                        outvars-decl
                                                        outvars))]
      (second (reduce pred-macro-updater
                      [replacements []]
                      raw-predicates)))))

(defn build-predicate-macro
  [invars outvars raw-predicates]
  (->> (build-predicate-macro-fn invars outvars raw-predicates)
       (p/predicate p/predicate-macro)))

(defn- to-jcascalog-fields [fields]
  (jcascalog.Fields. (or fields [])))

;; ## Predicate Macro Expansion
;;
;; This section deals with predicate macro expansion.

(defn- expand-predicate-macro
  [p vars]
  (let [{invars :<< outvars :>>} (parse/parse-variables vars :<)]
    (cond (var? p) [[(var-get p) vars]]
          (instance? Subquery p) [[(.getCompiledSubquery p) vars]]
          (instance? ClojureOp p) [(.toRawCascalogPredicate p vars)]
          (instance? PredicateMacro p) (-> p (.getPredicates
                                              (to-jcascalog-fields invars)
                                              (to-jcascalog-fields outvars)))
          (instance? PredicateMacroTemplate p) [[(.getCompiledPredMacro p) vars]]
          :else ((:pred-fn p) invars outvars))))

(defn expand-predicate-macros
  "Returns a sequence of predicates with all internal predicate macros
  expanded."
  [raw-predicates]
  (mapcat (fn [[p vars :as raw-pred]]
            (if (p/predicate-macro? p)
              (expand-predicate-macros (expand-predicate-macro p vars))
              [raw-pred]))
          raw-predicates))
