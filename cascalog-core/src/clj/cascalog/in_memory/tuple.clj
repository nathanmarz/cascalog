(ns cascalog.in-memory.tuple
  (:require [cascalog.logic.vars :as v]
            [flatland.ordered.map :as o]
            [jackknife.core :as u]))

(defn to-tuple
  [names v]
  (if (= (count names) (count v))
    (let [kv-pairs (map (fn [v1 v2] [v1 v2]) names v)]
      (into (o/ordered-map) kv-pairs))
    (u/throw-illegal "Output variables arity and function output arity do not match")))

(defn to-tuples
  "turns [\"n\"] and [[1] [2]] into [{\"n\" 1} {\"n\" 2}]"
  [names coll-of-seqs]
  (map #(to-tuple names %) coll-of-seqs))

(defn valid-tuple?
  "Verifies that non-nullable vars aren't null."
  [tuple]
  (not-any?
   (fn [[k v]]
     (and (v/non-nullable-var? k)
          (nil? v)))
   tuple))

(defn to-tuples-filter-nullable
  "turns [\"n\"] and [[1] [2]] into [{\"n\" 1} {\"n\" 2}]"
  [names coll-of-seqs]
  (->> coll-of-seqs
       (map
        (fn [s]
          (let [tuple (to-tuple names s)]
            (if (valid-tuple? tuple)
              tuple))))
       (remove nil?)))

(defn empty-tuple
  [fields]
  (to-tuple fields (repeat (count fields) nil)))

(defn select-fields
  "Creates a list of the values of the tuples you want and if the field isn't
   found, its value is the name of the field.
   For examples: (select-fields [:b :a 100] {:a 1 :b 2 :c 3}) => (2 1 100)"
  [fields tuple]
  (map #(get tuple % %) fields))

(defn extract-values
  "Creates a collection of vectors for the values of the fields
   you have selected"
  [fields tuples]
  (map #(vec (select-fields fields %))  tuples))

(defn tuple-sort
  [tuples sort-fields reverse?]
  (if sort-fields
    (let [sorted (sort-by #(vec (map % sort-fields)) tuples)]
      (if reverse?
        (reverse sorted)
        sorted))
    tuples))
