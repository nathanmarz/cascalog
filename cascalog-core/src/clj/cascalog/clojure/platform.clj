(ns cascalog.clojure.platform
  (:require [cascalog.logic.predicate]
            [cascalog.logic.platform :refer (compile-query  IPlatform)]
            [cascalog.logic.parse :as parse]
            [jackknife.core :as u]
            [jackknife.seq :as s]
            [cascalog.logic.def :as d])
  (:import [cascalog.logic.parse TailStruct Projection Application
            FilterApplication Unique Join Grouping Rename]
           [cascalog.logic.predicate Generator RawSubquery]
           [cascalog.logic.def ParallelAggregator ParallelBuffer]))

;; Generator
(defn to-tuple
  [names v]
  (zipmap names v))

(defn to-tuples
  "turns [\"n\"] and [[1] [2]] into [{\"n\" 1} {\"n\" 2}]"
  [names coll-of-seqs]
  (map #(to-tuple names %) coll-of-seqs))

(defn to-tuples-filter-nullable
  "turns [\"n\"] and [[1] [2]] into [{\"n\" 1} {\"n\" 2}]"
  [names coll-of-seqs]
  (remove nil? (map
                (fn [s]
                  (if (not-any? nil? s)
                    (to-tuple names s)))
                coll-of-seqs)))

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
(defn extract-values
  "Creates a collection of vectors for the values of the fields
   you have selected"
  [fields tuples]
  (map #(vec (select-fields fields %))  tuples))

(defn inner-join
  "Inner joins two maps that both have been grouped by the same function.
   This is an inner join, so nils are discarded."
  [l-grouped r-grouped]
  (letfn [(join-fn [l-group]
            (let [key (first l-group)
                  l-tuples (second l-group)]
              (if-let [r-tuples (get r-grouped key)]
                (for [x l-tuples y r-tuples] (merge x y)))))]
    (flatten ;; the for returned a collection which we need to flatten
     (remove nil? ;; nils are discarded
             (map join-fn l-grouped)))))

(defn left-join
  [l-grouped r-grouped r-fields]
  (letfn [(join-fn [l-group]
            (let [key (first l-group)
                  l-tuples (second l-group)
                  r-empty-tuples [(zipmap r-fields (repeat nil))]
                  r-tuples (get r-grouped key r-empty-tuples)]
              (for [x l-tuples y r-tuples]
                ;; merge is specifically ordered, because the left
                ;; tuple takes precedence over the right one (which
                ;; could be nil)
                (merge y x))))]
    (flatten ;; the for returned a collection which we need to flatten
     (remove nil? ;; nils are discarded
             (map join-fn l-grouped)))))

(defn left-excluding-join
  "A left join that only returns values where the right side is nil"
  [l-grouped r-grouped r-fields]
  (letfn [(join-fn [l-group]
            (let [key (first l-group)
                  l-tuples (second l-group)
                  r-empty-tuples [(zipmap r-fields (repeat nil))]]
              (if (not (find r-grouped key))
                (for [x l-tuples y r-empty-tuples]
                  ;; merge is specifically ordered, because the left
                  ;; tuple takes precedence over the right one (which
                  ;; could be nil)
                  (merge y x)))))]
    (flatten ;; the for returned a collection which we need to flatten
     (remove nil? ;; nils are discarded
             (map join-fn l-grouped)))))

(defn outer-join
  [l-grouped r-grouped l-fields r-fields]
  (let [inner (inner-join l-grouped r-grouped)
        left (left-excluding-join l-grouped r-grouped r-fields)
        right (left-excluding-join r-grouped l-grouped l-fields)]
    (concat inner left right)))

(defn join
  "Dispatches to all the different join types and returns a vector
   with the collection of joined tuples and the type of join"
  [l-grouped r-grouped l-type r-type l-fields r-fields]
  (cond
   (and (= :inner l-type) (= :inner r-type))
   [(inner-join l-grouped r-grouped) :inner]
   (and (= :inner l-type) (= :outer r-type))
   [(left-join l-grouped r-grouped r-fields) :outer]
   (and (= :outer l-type) (= :inner r-type))
   [(left-join r-grouped l-grouped l-type) :outer]
   :else [(outer-join l-grouped r-grouped l-fields r-fields) :outer]))

(defn smallest-arity [fun]
  "Returns the smallest number of arguments the function takes"
  (->> fun meta :arglists first count))

(defn tuple-sort
  [tuples sort-fields reverse?]
  (if sort-fields
    (let [sorted (sort-by #(vec (map % sort-fields)) tuples)]
      (if reverse?
        (reverse sorted)
        sorted))
    tuples))

(defmulti agg-clojure
  (fn [coll op]
    (type op)))

(defmethod agg-clojure ::d/buffer
  [coll op]
  (op coll))

(defmethod agg-clojure ::d/bufferiter
  [coll op]
  ;; coll is a lazy-seq but the operatation expects an iterator
  ;; so we need to convert it to one
  (op (.iterator coll)))

(defmethod agg-clojure ::d/aggregate
  [coll op]
  (op (reduce (fn [s v] (op s (first v))) (op) coll)))

(defmethod agg-clojure ParallelAggregator
  [coll {:keys [init-var combine-var]}]
  (let [mapped-coll (map
                  #(apply init-var
                          (take (smallest-arity init-var) %))
                  coll)]
    (reduce combine-var mapped-coll)))

(defmethod agg-clojure ParallelBuffer
  [coll {:keys [init-var combine-var present-var buffer-var]}]
  (let [init-coll (mapcat #(init-var %) coll )
        combined-coll (combine-var nil init-coll)
        reduced-coll (present-var (first combined-coll))
        tuples (map #(apply concat %) reduced-coll)
        buffered-tuples (buffer-var tuples)]
    buffered-tuples))

(defprotocol IRunner
  (to-generator [item]))

;; Extend these types to allow for different types of querying: joins,
;; filters, aggregation, etc.  You need to extend all the types that
;; implement defnode in logic.parse, otherwise you won't implement the
;; full query capailities
(extend-protocol IRunner
  Projection
  (to-generator [{:keys [source fields]}]
    ;; TODO: this is a hacky way of filtering the tuple to just the
    ;; fields we want
    (->> (extract-values fields source)
         (remove nil?)
         (to-tuples fields)))

  Generator
  (to-generator [{:keys [gen]}] gen)

  Rename
  (to-generator [{:keys [source fields]}]
    (map
     (fn [tuple]
       ;; TODO: extracting the vals from a map and assuming they have
       ;; a specific order is dangerous since the order could be
       ;; different than the expected tuple order
       (let [vals (map (fn [[k v]] v) tuple)]
         (zipmap fields vals)))
     source))

  Application
  (to-generator [{:keys [source operation]}]
    (let [{:keys [op input output]} operation]
      (map
       (fn [tuple]
         (let [v (s/collectify (apply op (select-fields-w-default input tuple)))
               new-tuple (to-tuple output v)]
              (merge tuple new-tuple)))
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
      (tuple-sort tuples sort reverse)))
  
  Join
  (to-generator [{:keys [sources join-fields type-seq options]}]
    (loop [loop-sources sources
           loop-join-fields join-fields
           loop-type-seq type-seq]
      (let [
            ;; extract the vars we want
            [l-source r-source & rest-sources] loop-sources
            [l-type-seq r-type-seq & rest-type-seqs] loop-type-seq
            [l-fields l-type] l-type-seq
            [r-fields r-type] r-type-seq

            ;; setup the data for joining
            l-grouped (group-by #(vec (map % join-fields)) l-source)
            r-grouped (group-by #(vec (map % join-fields)) r-source)

            ;; join the data and setup the vars for the next loop
            j-fields (distinct (concat l-fields r-fields))
            [j-source j-type] (join l-grouped r-grouped
                                    l-type r-type
                                    l-fields r-fields)]
        ;; only recur if there are more sources to join
        (if (empty? rest-sources)
          j-source
          (recur
           (cons j-source rest-sources)
           loop-join-fields
           (cons [j-fields j-type] rest-type-seqs))))))

  Grouping
  (to-generator [{:keys [source aggregators grouping-fields options]}]
    (let [{:keys [sort reverse]} options
          grouped (group-by #(vec (map % grouping-fields)) source)]
      (mapcat
       (fn [[grouping-vals tuples]]
         (let [sorted-tuples (tuple-sort tuples sort reverse)
               agg-tuples (map
                           (fn [{:keys [op input output]}]
                             (let [coll (extract-values input sorted-tuples)
                                   r (s/collectify (agg-clojure coll op))
                                   r-seq (if (sequential? (first r)) r [r])]
                               (map #(zipmap output %) r-seq)))
                           aggregators)
               merged-tuples (loop [[s1 s2 & s-rest] agg-tuples]
                               (if (or (empty? s1) (empty? s2))
                                 (concat s1 s2)
                                 (let [s-merge (for [x s1 y s2] (merge x y))]
                                   (if (empty? s-rest)
                                     s-merge
                                     (recur (cons s-merge s-rest))))))]
           (map #(merge (zipmap grouping-fields grouping-vals) %) merged-tuples)))
       grouped)))
  
  TailStruct
  (to-generator [{:keys [node available-fields]}]
    ;; TODO: if we want the fields on the structure, we don't need to
    ;; extract the values
    (extract-values available-fields node)))

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
    (to-tuples-filter-nullable output (generator gen)))

  (to-generator [_ x]
    (to-generator x)))
