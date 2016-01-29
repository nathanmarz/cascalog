(ns cascalog.in-memory.platform
  "The In-Memory platform enables Cascalog to query data without the
   need for Hadoop.  Instead, the data is manipulated entirely in-memory
   using just Clojure.

   First, Cascalog converts the generators into tuples (which are just a
   sequence of maps).  Cascalog then maps, filters, and aggregates
   the tuples.  Finally, it uses sinks to output the resulting tuples."
  (:refer-clojure :exclude [run!])
  (:require [cascalog.logic.predicate]
            [cascalog.logic.platform :as p]
            [cascalog.logic.parse :as parse]
            [cascalog.in-memory.join :refer (join)]
            [cascalog.in-memory.util :refer (smallest-arity system-println)]
            [cascalog.in-memory.tuple :as t]
            [cascalog.logic.def :as d]
            [jackknife.core :as u]
            [jackknife.seq :as s])
  (:import [cascalog.logic.parse TailStruct Projection Application
            FilterApplication Unique Join Grouping Rename ExistenceNode]
           [cascalog.logic.predicate Generator]
           [cascalog.logic.def ParallelAggregator ParallelBuffer]
           [jcascalog Subquery]))

(defprotocol ISink
  (to-sink [sink tuples fields]))

;; ## Platform

(defrecord InMemoryPlatform []
  p/IPlatform
  (generator? [_ x]
    (p/platform-generator? x))

  (generator-builder [_ gen output options]
    (t/to-tuples-filter-nullable output (p/generator gen)))

  (run! [p _ bindings]
    (doall
     (map (fn [[sink query]]
            (let [available-fields (parse/get-out-fields query)
                  tuples (p/compile-query query)]
              (to-sink sink tuples available-fields)))
          (partition 2 bindings)))
    nil)

  (run-to-memory! [_ _ queries]
    (map
     (fn [query]
       (let [available-fields (parse/get-out-fields query)
             tuples (p/compile-query query)]
         (t/map-select-values available-fields tuples)))
     queries)))

;; Sinks

(defrecord StdOutSink [])

(extend-protocol ISink
  clojure.lang.Atom
  (to-sink [a tuples fields]
    (reset! a tuples))

  clojure.lang.IFn
  (to-sink [f tuples fields]
    (f tuples fields))

  StdOutSink
  (to-sink [_ tuples fields]
    (system-println "")
    (system-println "")
    (system-println "RESULTS")
    (system-println "-----------------------")
    (doseq [t tuples]
      (apply system-println (t/select-values fields t)))
    (system-println "-----------------------")))

;; ## Generators

(defmethod p/generator [InMemoryPlatform clojure.lang.IPersistentVector]
  [v]
  (p/generator (or (seq v) ())))

(defmethod p/generator [InMemoryPlatform clojure.lang.ISeq]
  [v]
  (map s/collectify v))

(defmethod p/generator [InMemoryPlatform java.util.ArrayList]
  [coll]
  (p/generator (into [] coll)))

(defmethod p/generator [InMemoryPlatform TailStruct]
  [sq]
  (let [available-fields (parse/get-out-fields sq)
        tuples (p/compile-query sq)]
    (t/map-select-values available-fields tuples)))

;; ## To Generators

(defmethod p/to-generator [InMemoryPlatform Subquery]
  [sq]
  (p/compile-query (.getCompiledSubquery sq)))

(defmethod p/to-generator [InMemoryPlatform Projection]
  [{:keys [source fields]}]
  (t/project source fields))

(defmethod p/to-generator [InMemoryPlatform Generator]
  [{:keys [gen]}] gen)

(defmethod p/to-generator [InMemoryPlatform Rename]
  [{:keys [source input output]}]
  (t/project source input output))

(defmulti op-clojure
  (fn [coll op input output]
    (type op)))

(defmethod p/to-generator [InMemoryPlatform Application]
  [{:keys [source operation]}]
  (let [{:keys [op input output]} operation]
    (op-clojure source op input output)))

(defmethod p/to-generator [InMemoryPlatform FilterApplication]
  [{f :filter source :source}]
  (let [{:keys [op input]} f]
    (filter
     #(apply op (t/select-values input %))
     source)))

(defmethod p/to-generator [InMemoryPlatform ExistenceNode]
  [{:keys [source] :as a}]
  source)

(defmethod p/to-generator [InMemoryPlatform Unique]
  [{:keys [source fields options]}]
  (let [{:keys [sort reverse]} options
        coll (map #(t/select-values fields %) source)
        distinct-coll (distinct coll)
        tuples (t/to-tuples fields distinct-coll)]
    (t/sort tuples sort reverse)))

(defmethod p/to-generator [InMemoryPlatform Join]
  [{:keys [sources join-fields type-seq options]}]
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

(defmulti agg-clojure
  "Allow various types of aggregate operations for the collection.
   Cascalog aggregate types are defined in cascalog.logic.def."
  (fn [coll op]
    (type op)))

(defn aggregate-tuples [aggregators sorted-tuples]
  (map
   (fn [{:keys [op input output]}]
     (let [coll (t/map-select-values input sorted-tuples)
           r (s/collectify (agg-clojure coll op))
           r-seq (if (sequential? (first r)) r [r])]
       (map #(t/to-tuple output %) r-seq)))
   aggregators))

(defmethod p/to-generator [InMemoryPlatform Grouping]
  [{:keys [source aggregators grouping-fields options]}]
  (let [{:keys [sort reverse]} options]
    (->> source
         (group-by #(vec (map % grouping-fields)))
         (mapcat
          (fn [[grouping-vals tuples]]
            (let [original-tuple (t/to-tuple grouping-fields grouping-vals)]
              (->> (t/sort tuples sort reverse)
                   (aggregate-tuples aggregators)
                   (t/cross-join)
                   (map #(merge original-tuple %)))))))))

(defmethod p/to-generator [InMemoryPlatform TailStruct]
  [{:keys [node available-fields]}]
  (t/project node available-fields))

;; ## Application Helpers

(defmethod op-clojure ::d/map
  [coll op input output]
  (map
   (fn [tuple]
     (let [v (s/collectify (apply op (t/select-values input tuple)))
           new-tuple (t/to-tuple output v)]
       (merge tuple new-tuple)))
   coll))

(defmethod op-clojure ::d/mapcat
  [coll op input output]
  (mapcat
   (fn [tuple]
     (let [v (apply op (t/select-values input tuple))
           new-tuples (map #(t/to-tuple output (s/collectify %)) v)]
       (map #(merge tuple %) new-tuples)))
   coll))

;; ## Grouping Helpers

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
  [coll op]
  (let [{:keys [combine-var init-var present-var]} op
        mapped-coll (map
                     #(->> %
                           (take (smallest-arity init-var))
                           (apply init-var))
                     coll)
        reduced-coll (reduce
                      (fn [state s]
                        (->> s
                             (s/collectify)
                             (apply conj (s/collectify state))
                             (apply combine-var)))
                      (first mapped-coll)
                      (rest mapped-coll))]
    reduced-coll))

(defmethod agg-clojure ParallelBuffer
  [coll {:keys [init-var combine-var present-var buffer-var]}]
  (buffer-var coll))
