(ns cascalog.clojure.platform
  (:require [cascalog.logic.predicate]
            [cascalog.logic.platform :refer (compile-query  IPlatform)]
            [cascalog.logic.parse :as parse]
            [jackknife.core :as u])
  (:import [cascalog.logic.parse TailStruct Projection Application
            FilterApplication Unique]
           [cascalog.logic.predicate Generator RawSubquery]))

;; Generator
(defn to-tuple
  [names v]
  (zipmap names v))

(defn to-tuples
  "turns [\"n\"] and [[1] [2]] into [{\"n\" 1} {\"n\" 2}]"
  [names coll-of-seqs]
  (map #(to-tuple names %) coll-of-seqs))

(defn select-fields
  "Creates a list of the values of the tuples you want and if the field isn't
   found it's nil.
   For examples: (select-fields [:b :a :d] {:a 1 :b 2 :c 3}) => (2 1 nil)"
  [fields tuple]
  (map #(tuple %) fields))

(defn select-fields-w-default
  "Just like select-fields but if a value isn't found, its value is
   the name of the field.
   For examples: (select-fields [:b :a 100] {:a 1 :b 2 :c 3}) => (2 1 100)"
  [fields tuple]
  (map #(get tuple % %) fields))

;; Projection
(defn extract-fields
  "Creates a collection of vectors for the fields you have selected"
  [fields tuples]  
  (remove nil?
          (map #(vec (select-fields fields %))
               tuples)))

(defprotocol IRunner
  (to-generator [item]))

;; Extend these types to allow for different types of querying: joins,
;; filters, aggregation, etc.  You need to extend all the types that
;; implement defnode in logic.parse, otherwise you won't implement the
;; full query capailities
(extend-protocol IRunner
  Projection
  (to-generator [{:keys [source fields]}]
    (extract-fields fields source))

  Generator
  (to-generator [{:keys [gen]}] gen)

  Application
  (to-generator [{:keys [source operation]}]
    (let [{:keys [op input output]} operation]
      (map
       #(to-tuple output (list (apply op (select-fields input %))))
       source)))

  FilterApplication
  (to-generator [{:keys [source filter]}]
    (let [{:keys [op input]} filter]
      (clojure.core/filter
       #(apply op (select-fields-w-default input %))
       source)))

  Unique
  (to-generator [{:keys [source fields options]}]
    (let [{:keys [sort reverse]} options
          coll (map #(select-fields fields %) source)
          distinct-coll (distinct coll)
          tuples (to-tuples fields distinct-coll)]
      (if sort
        (let [sorted (sort-by #(vec (map % sort)) tuples)]
          (if reverse
            (clojure.core/reverse sorted)
            sorted))
        tuples)))
  
  ;; this type is standard and could be part of the base logic
  TailStruct
  (to-generator [item]
    (:node item)))

(defprotocol IGenerator
  (generator [x]))

(extend-protocol IGenerator
  
  ;; A bunch of generators that finally return  a seq
  clojure.lang.IPersistentVector
  (generator [v]
    (generator (or (seq v) ())))
  
  clojure.lang.ISeq
  (generator [v] v)

  java.util.ArrayList
  (generator [coll]
    (generator (into [] coll)))

  ;; These generators act differently than the ones above
  TailStruct
  (generator [sq]
    (compile-query sq))

  RawSubquery
  (generator [sq]
    (generator (parse/build-rule sq))))

(defrecord ClojurePlatform []
  IPlatform
  (generator? [_ x]
    (satisfies? IGenerator x))

  (generator [_ gen output options]
    (to-tuples output (generator gen)))

  (to-generator [_ x]
    (to-generator x)))
