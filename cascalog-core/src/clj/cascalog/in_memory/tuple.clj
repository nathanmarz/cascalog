(ns cascalog.in-memory.tuple
  "Tuples encapsulate the data that Cascalog queries.  An individual
   tuple contains a field name and a value.  It is represented as a map.
   So tuples are just a sequence of maps, and an example is:
   [{'?num' 1} {'?num' 2}]."
  (:refer-clojure :exclude [sort])
  (:require [cascalog.logic.vars :as v]
            [jackknife.core :as u]))

(defn to-tuple
  [names v]
  (if (= (count names) (count v))
    (zipmap names v)
    (u/throw-illegal "Output variables arity and function output arity do not match")))

(defn to-tuples
  "turns [\"n\"] and [[1] [2]] into [{\"n\" 1} {\"n\" 2}]"
  [names coll-of-seqs]
  (map #(to-tuple names %) coll-of-seqs))

(defn valid?
  "Verifies that non-nullable vars aren't null."
  [tuple]
  (not-any?
   (fn [[k v]]
     (and (v/non-nullable-var? k)
          (nil? v)))
   tuple))

(defn to-tuples-filter-nullable
  "Turns [\"n\"] and [[1] [2]] into [{\"n\" 1} {\"n\" 2}]"
  [names coll-of-seqs]
  (->> coll-of-seqs
       (map
        (fn [s]
          (let [tuple (to-tuple names s)]
            (if (valid? tuple)
              tuple))))
       (remove nil?)))

(defn empty-tuple
  "Creates a tuple with a nil value for all of the fields"
  [fields]
  (to-tuple fields (repeat (count fields) nil)))

(defn select-values
  "Creates a list of the values of the tuples you want and if the field isn't
   found, its value is the name of the field.
   For examples: (select-values [:b :a 100] {:a 1 :b 2 :c 3}) => (2 1 100)"
  [fields tuple]
  (map #(get tuple % %) fields))

(defn map-select-values
  "Creates a collection of vectors for the values of the fields
   you have selected"
  [fields tuples]
  (map #(select-values fields %) tuples))

(defn sort
  [tuples sort-fields reverse?]
  (if sort-fields
    (let [sorted (sort-by #(vec (select-values sort-fields %)) tuples)]
      (if reverse?
        (reverse sorted)
        sorted))
    tuples))

(defn cross-join
  "Input a collection of a collection of tuples like [[{:b 2}] [{:a 1} {:a 3}]
   And you'll get a result like: [{:a 1 :b 2} {:a 3 :b 2}]"
  [coll-of-tuples]
  (loop [[s1 s2 & s-rest] coll-of-tuples]
    (if (or (empty? s1) (empty? s2))
      (concat s1 s2)
      (let [s-merge (for [x s1 y s2] (merge x y))]
        (if (empty? s-rest)
          s-merge
          (recur (cons s-merge s-rest)))))))

(defn project
  ([tuples fields] (project tuples fields fields))
  ([tuples input-fields output-fields]
     (map
      #(->> %
            (select-values input-fields)
            (to-tuple output-fields))
      tuples)))
