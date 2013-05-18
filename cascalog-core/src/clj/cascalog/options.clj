(ns cascalog.options
  (:require [jackknife.core :refer (throw-illegal)]))

;; ## Option Parsing
;;
;; The following code deals with parsing of Cascalog's option
;; predicates. The goal is to accept the options for a particular
;; subquery and generate a map of supported option -> the option's
;; value.
;;
;; A couple of nice extensions would be:
;;
;; * Validation for each option type.
;; * Can the user add an option type on the fly? Is there some generic
;; way to specify an option's meaning within the predicate? Not sure
;;how we can make this pluggable.

(def DEFAULT-OPTIONS
  "The set of options supported by Cascalog, mapped to default values."
  {:distinct false
   :sort nil
   :reverse false
   :trap nil})

(defn careful-merge
  "Monoid that keeps the right value of it's not nil or not equal to
  the old left value. If these conditions aren't met, the merge will
  throw an exception."
  [l r]
  (if (or (nil? r) (= l r))
    (throw-illegal "Same option set to conflicting values!")
    r))

(defn generate-option-map
  "Accepts a sequence of option predicates and generates a map of
  option -> value."
  [opt-predicates]
  (->> opt-predicates
       (map (fn [[opt & more]]
              (assert (contains? DEFAULT-OPTIONS opt)
                      (str opt " is not a valid option predicate"))
              {opt (condp = opt
                     :sort (flatten more)
                     :trap (first more)
                     (first more))}))
       (apply merge-with careful-merge)
       (merge DEFAULT-OPTIONS)))
