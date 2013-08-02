(ns cascalog.logic.predmacro
  "This namespace contains functions that help to define predicate
   macro instances, and compile predicate macro instances out into
   sequences of RawPredicate instances."
  (:require [clojure.set :refer (intersection)]
            [clojure.walk :refer (postwalk)]
            [cascalog.logic.predicate :as p]
            [cascalog.logic.vars :as v]
            [jackknife.core :as u]
            [jackknife.seq :as s])
  (:import [jcascalog PredicateMacro PredicateMacroTemplate]))

;; ## Predicate Macro Building Functions

;; "expand" is called from "normalize" in cascalog.parse. The parsing
;;  code takes care of the recursive expansion needed on the results
;;  of a call to "expand".

(defprotocol IPredMacro
  (expand [_ input output]
    "Returns a sequence of vectors suitable to feed into
    cascalog.parse/normalize."))

(defn predmacro? [o]
  (satisfies? IPredMacro (if (var? o) @o o)))

(extend-protocol p/ICouldFilter
  cascalog.logic.predmacro.IPredMacro
  (filter? [_] true))

(extend-protocol IPredMacro
  ;; Predicate macro templates really should just extend this protocol
  ;; directly. getCompiledPredMacro calls into build-predmacro below
  ;; and returns a reified instance of IPredMacro.
  PredicateMacroTemplate
  (expand [p input output]
    (expand (.getCompiledPredMacro p) input output))

  clojure.lang.Var
  (expand [v input output]
    (if (predmacro? v)
      (expand @v input output)
      (u/throw-runtime)))

  ;; TODO: jCascalog shold just use these interfaces directly. If this
  ;; were the case, we wouldn't have to extend the protocol here.
  PredicateMacro
  (expand [p input output]
    (letfn [(to-fields [fields]
              (jcascalog.Fields. (or fields [])))]
      (-> p (.getPredicates (to-fields input)
                            (to-fields output))))))

;; kind of a hack, simulate using pred macros like filters

(defn use-as-filter?
  "If a predicate macro had a single output variable defined and you
  try to use it with no output variables, the predicate macro acts as
  a filter."
  [output-decl outvars]
  (and (empty? outvars)
       (sequential? output-decl)
       (= 1 (count output-decl))))

(defn predmacro*
  "Functional version of predmacro. See predmacro for details."
  [fun]
  (reify IPredMacro
    (expand [_ invars outvars]
      (fun invars outvars))))

(defmacro predmacro
  "A more general but more verbose way to create predicate macros.

   Creates a function that takes in [invars outvars] and returns a
   list of predicates. When making predicate macros this way, you must
   create intermediate variables with gen-nullable-var(s). This is
   because unlike the (<- [?a :> ?b] ...) way of doing pred macros,
   Cascalog doesn't have a declaration for the inputs/outputs.

   See https://github.com/nathanmarz/cascalog/wiki/Predicate-macros
  "
  [& body]
  `(predmacro* (fn ~@body)))

(defn validate-declarations!
  "Assert that the same variables aren't used on input and output when
  defining a predicate macro."
  [input-decl output-decl]
  (when (seq (intersection (set input-decl)
                           (set output-decl)))
    ;; TODO: ignore destructuring characters and check that no
    ;; constants are present.
    (u/throw-runtime (format
                      (str "Cannot declare the same var as "
                           "an input and output to predicate macro: %s %s")
                      input-decl output-decl))))

(defn build-predmacro
  "Build a predicate macro via input and output declarations. This
  function takes a sequence of declared inputs, a seq of declared
  outputs and a sequence of raw predicates. Upon use, any variable
  name not in the input or output declarations will be replaced with a
  random Cascalog variable (uniqued by appending a suffix, so nullable
  vs non-nullable will be maintained)."
  [input-decl output-decl raw-predicates]
  (validate-declarations! (when input-decl
                            (s/collectify input-decl))
                          (when output-decl
                            (s/collectify output-decl)))
  (reify IPredMacro
    (expand [_ invars outvars]
      (let [outvars (if (use-as-filter? output-decl outvars)
                      [true]
                      outvars)
            replacement-m (s/mk-destructured-seq-map input-decl invars
                                                     output-decl outvars)
            update (memoize (fn [v]
                              (if (v/cascalog-var? v)
                                (replacement-m (str v) (v/uniquify-var v))
                                v)))]
        (->> raw-predicates
             (mapcat (fn [pred]
                       (map (fn [{:keys [input output] :as p}]
                              (-> p
                                  (assoc :input  (postwalk update input))
                                  (assoc :output (postwalk update output))))
                            (p/normalize pred)))))))))
