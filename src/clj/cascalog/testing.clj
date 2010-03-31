(ns cascalog.testing
  (:use clojure.test
        clojure.contrib.java-utils
        cascalog.io)
  (:import (cascading.tuple Fields Tuple TupleEntry TupleEntryCollector)
           (cascading.pipe Pipe)
           (cascading.operation ConcreteCall)
           (cascading.flow FlowProcess)
           (cascalog Util ClojureMap)
           (java.lang Comparable)
           (clojure.lang IPersistentCollection)
           (org.apache.hadoop.mapred JobConf))
  (:require (cascalog [workflow :as w])))

(defn- roundtrip [obj]
  (cascading.util.Util/deserializeBase64
    (cascading.util.Util/serializeBase64 obj)))

(defn invoke-filter [fil coll]
  (let [fil     (roundtrip fil)
        op-call (ConcreteCall.)
        fp-null FlowProcess/NULL]
    (.setArguments op-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare fil fp-null op-call)
    (let [rem (.isRemove fil fp-null op-call)]
      (.cleanup fil fp-null op-call)
      rem)))

(defn- output-collector [out-atom]
  (proxy [TupleEntryCollector] []
    (add [tuple]
      (swap! out-atom conj (Util/coerceFromTuple tuple)))))

(defn- op-call []
  (let [args-atom    (atom nil)
        out-atom     (atom [])
        context-atom (atom nil)]
    (proxy [ConcreteCall IPersistentCollection] []
      (setArguments [tuple]
        (swap! args-atom (constantly tuple)))
      (getArguments []
         @args-atom)
      (getOutputCollector []
        (output-collector out-atom))
      (setContext [context]
        (swap! context-atom (constantly context)))
      (getContext []
        @context-atom)
      (seq []
        (seq @out-atom)))))

(defn- op-call-results [func-call]
  (.seq func-call))

(defn invoke-function [m coll]
  (let [m         (roundtrip m)
        func-call (op-call)
        fp-null   FlowProcess/NULL]
    (.setArguments func-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare m fp-null func-call)
    (.operate m fp-null func-call)
    (.cleanup m fp-null func-call)
    (op-call-results func-call)))

(defn invoke-aggregator [a colls]
  (let [a       (roundtrip a)
        ag-call (op-call)
        fp-null FlowProcess/NULL]
    (.prepare a fp-null ag-call)
    (.start a fp-null ag-call)
    (doseq [coll colls]
      (.setArguments ag-call (TupleEntry. (Util/coerceToTuple coll)))
      (.aggregate a fp-null ag-call))
    (.complete a fp-null ag-call)
    (.cleanup  a fp-null ag-call)
    (op-call-results ag-call)))

(defn mk-test-tap [fields-def path]
  (w/lfs-tap (w/sequence-file fields-def) path))

(defn mk-test-source [[fields-def data] path]
  (let [source (mk-test-tap fields-def path)]
    (with-open [collector (.openForWrite source (JobConf.))]
      (doall (map #(.add collector (Util/coerceToTuple %)) data)))
    source ))

(defn mk-test-sink [[fields-def data] path]
  [(mk-test-tap fields-def path) data])

(defn unique-rooted-paths [root]
  (map str (cycle [(str root "/")]) (iterate inc 0)))

(defn transpose [m]
  (vec (apply map vector m)))

(defn get-tuples [sink]
  (with-open [it (.openForRead sink (JobConf.))]
       (doall (map #(Util/coerceFromTuple (Tuple. (.getTuple %))) (iterator-seq it)))))


; (defn- normalize-test-data [data]
;   (letfn
;     [(normalize-helper [item]
;       (if (sequential? item))
;     )]
;     )
;   "
;   ['word' ['a' 'b' 'c']]
;   [['word'] ['a' 'b' 'c']]
;   [[['word'] ['a' 'b' 'c']]]
;   [['word' ['a' 'b' 'c']]]
;   [[['word'] [['a'] ['b'] ['c']]] [['field1' 'field2'] [['a' '1'] ['b' '2'] ['c' '3']]]]
;   
;   look down first elem until last one where you have a pair where the second is a vector
;   first will be fields, second will be data for that source. just need to determine how far down is the 
;   last 
;   -> return [fields data]
;   "
;   )

;; source-data is a vector of [fields data-vector], i.e. [["field1", "field2"], [[1, 2] [2, 3] [3, 4]]]
;; sink-data is a vector of [fields data-vector], i.e. [["field1", "field2"], [[1, 2] [2, 3] [3, 4]]]
(defn test-flow [source-data assembly sink-data]
  (with-log-level :warn
      (with-tmp-files [source-dir-path (temp-dir "source")
                       sink-path (temp-path "sink")]
            (let
              [sources (doall (map mk-test-source source-data (unique-rooted-paths source-dir-path)))
               [sinks expected-results] (transpose (doall (map mk-test-sink sink-data (unique-rooted-paths sink-path))))
               flow (w/mk-flow sources sinks assembly)
               _ (w/exec flow)
               out-tuples (doall (map get-tuples sinks))]
               (println out-tuples)
               ;; now make sure results of all sinks corresponds to their expected results
               ;; use a multiset
               (is (every? (fn [[a b]] (= a b)) (transpose [out-tuples expected-results])))))))
