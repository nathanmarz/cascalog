(ns cascalog.logic.options
  (:require [jackknife.core :refer (throw-illegal uuid)]
            [jackknife.seq :as s]))

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
;;   way to specify an option's meaning within the predicate? Not sure
;;   how we can make this pluggable, other than just allowing all options.

(def DEFAULT-OPTIONS
  "The set of options supported by Cascalog, mapped to default values."
  {:distinct false
   :sort nil
   :reverse nil
   :trap nil
   :spill-threshold nil
   :reducers nil
   :name ""
   :stats-fn nil})

(defn careful-merge
  "Semigroup that keeps the right value of it's not nil or not equal
  to the old left value. If these conditions aren't met, the merge
  will throw an exception."
  [l r]
  (if-not (or (nil? l) (= l r))
    (throw-illegal (format "Same option set to conflicting values: %s vs %s."
                           l r))
    r))

(def option?
  "A predicate is an option if it begins with a keyword."
  (comp keyword? :op))

(defn generate-option-map
  "Accepts a sequence of option predicates and generates a map of
  option -> value."
  [opt-predicates]
  (->> opt-predicates
       (map (fn [{:keys [op input output]}]
              (assert (contains? DEFAULT-OPTIONS op)
                      (str op " is not a valid option predicate"))
              {op (condp = op
                    ;; Flatten sorting fields.
                    :sort (flatten input)
                    ;; TODO: validation.
                    :trap {:tap (first input) :name (uuid)}
                    ;; Otherwise, take the first item. TODO: Throw if
                    ;; more than one item exists for non-sorting
                    ;; fields.
                    (first input))}))
       (apply merge-with careful-merge)
       (merge DEFAULT-OPTIONS)))

(defn extract-options
  "Accepts a sequence of raw predicates and returns a 2-vector of
  [option-map, rest-of-preds]."
  [preds]
  (let [[raw-options preds] (s/separate option? preds)]
    [(generate-option-map raw-options) preds]))
