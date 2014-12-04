(ns cascalog.in-memory.platform
  (:require [cascalog.logic.predicate]
            [cascalog.logic.platform :refer
             (compile-query IPlatform platform-generator? generator to-generator)]
            [cascalog.logic.parse :as parse]
            [cascalog.in-memory.join :refer (join)]
            [cascalog.in-memory.tuple :refer :all]
            [cascalog.in-memory.util :refer (smallest-arity system-println)]
            [cascalog.logic.def :as d]
            [jackknife.core :as u]
            [jackknife.seq :as s])
  (:import [cascalog.logic.parse TailStruct Projection Application
            FilterApplication Unique Join Grouping Rename]
           [cascalog.logic.predicate Generator RawSubquery]
           [cascalog.logic.def ParallelAggregator ParallelBuffer]
           [jcascalog Subquery]))

(defprotocol ISink
  (to-sink [sink tuples fields]))

;; Platform

(defrecord InMemoryPlatform []
  IPlatform
  (generator? [_ x]
    (platform-generator? x))

  (generator-builder [_ gen output options]
    (to-tuples-filter-nullable output (generator gen)))

  (run! [p _ bindings]
    (map (fn [[sink query]]
           (let [[tuples available-fields] (compile-query query)]
             (to-sink sink tuples available-fields)))
         (partition 2 bindings)))

  (run-to-memory! [_ _ queries]
    (map
     (fn [query]
       (let [[tuples available-fields] (compile-query query)]
         (extract-values available-fields tuples)))
     queries)))

;; Sinks

(defrecord StdOutSink [])

(extend-protocol ISink
  clojure.lang.Atom
  (to-sink [a tuples fields]
    (reset! a tuples))

  StdOutSink
  (to-sink [_ tuples fields]
    (system-println "")
    (system-println "")
    (system-println "RESULTS")
    (system-println "-----------------------")
    (doseq [t tuples]
      (apply system-println (select-fields fields t)))
    (system-println "-----------------------")))

;; Generators

(defmethod generator [InMemoryPlatform clojure.lang.IPersistentVector]
  [v]
  (generator (or (seq v) ())))

(defmethod generator [InMemoryPlatform clojure.lang.ISeq]
  [v] v) 

(defmethod generator [InMemoryPlatform java.util.ArrayList]
  [coll]
  (generator (into [] coll)))

(defmethod generator [InMemoryPlatform TailStruct]
  [sq]
  (compile-query sq))

;; To Generators

(defmethod to-generator [InMemoryPlatform Subquery]
  [sq]
  (generator (.getCompiledSubquery sq)))

(defmethod to-generator [InMemoryPlatform Projection]
  [{:keys [source fields]}]
  (project-tuples source fields))

(defmethod to-generator [InMemoryPlatform Generator]
  [{:keys [gen]}] gen)

(defmethod to-generator [InMemoryPlatform Rename]
  [{:keys [source input output]}]
  (project-tuples source input output))

(defmulti op-clojure
  (fn [coll op input output]
    (type op)))

(defmethod to-generator [InMemoryPlatform Application]
  [{:keys [source operation]}]
  (let [{:keys [op input output]} operation]
    (op-clojure source op input output)))

(defmethod to-generator [InMemoryPlatform FilterApplication]
  [{f :filter source :source}]
  (let [{:keys [op input]} f]
    (filter
     #(apply op (select-fields input %))
     source)))

(defmethod to-generator [InMemoryPlatform Unique]
  [{:keys [source fields options]}]
  (let [{:keys [sort reverse]} options
        coll (map #(select-fields fields %) source)
        distinct-coll (distinct coll)
        tuples (to-tuples fields distinct-coll)]
    (tuple-sort tuples sort reverse)))

(defmethod to-generator [InMemoryPlatform Join]
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
  (fn [coll op]
    (type op)))

(defn aggregate-tuples [aggregators sorted-tuples]
  (map
   (fn [{:keys [op input output]}]
     (let [coll (extract-values input sorted-tuples)
           r (s/collectify (agg-clojure coll op))
           r-seq (if (sequential? (first r)) r [r])]
       (map #(to-tuple output %) r-seq)))
   aggregators))

(defmethod to-generator [InMemoryPlatform Grouping]
  [{:keys [source aggregators grouping-fields options]}]
  (let [{:keys [sort reverse]} options]
    (->> source
         (group-by #(vec (map % grouping-fields)))
         (mapcat
          (fn [[grouping-vals tuples]]
            (let [original-tuple (to-tuple grouping-fields grouping-vals)]
              (->> (tuple-sort tuples sort reverse)
                   (aggregate-tuples aggregators)
                   (cross-join-tuples)
                   (map #(merge original-tuple %)))))))))

(defmethod to-generator [InMemoryPlatform TailStruct]
  [{:keys [node available-fields]}]
  ;; This is the last to-gerenator, so the tuples and their
  ;; field list are returned to enable the caller to
  ;; turn the tuples into a seq of just values.
  [(project-tuples node available-fields) available-fields])

;; Application Helpers

(defmethod op-clojure ::d/map
  [coll op input output]
  (map
   (fn [tuple]
     (let [v (s/collectify (apply op (select-fields input tuple)))
           new-tuple (to-tuple output v)]
       (merge tuple new-tuple)))
   coll))

(defmethod op-clojure ::d/mapcat
  [coll op input output]
  (mapcat
   (fn [tuple]
     (let [v (apply op (select-fields input tuple))
           new-tuples (map #(to-tuple output (s/collectify %)) v)]
       (map #(merge tuple %) new-tuples)))
   coll))

;; Grouping Helpers

(defmethod agg-clojure ::d/buffer
  [coll op]
  (if (d/prepared? op)
    (let [
          ;; Allows Cascading to pass in a
          ;; FlowProcess and an OperationCall,
          ;; but we'll just use nils
          fun (op nil nil)
          operation (:operate fun)]
      (operation coll))
    (op coll)))

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

