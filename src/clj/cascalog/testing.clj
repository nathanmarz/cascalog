(ns cascalog.testing
  (:use clojure.test
        clojure.contrib.java-utils
        cascalog.io
        cascalog.util)
  (:import [cascading.tuple Fields Tuple TupleEntry TupleEntryCollector]
           [cascading.pipe Pipe]
           [cascading.operation ConcreteCall]
           [cascading.flow FlowProcess]
           [cascalog Util ClojureMap]
           [java.lang Comparable]
           [clojure.lang IPersistentCollection]
           [org.apache.hadoop.mapred JobConf])
  (:require [cascalog [workflow :as w]]))

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

(defn unique-rooted-paths [root]
  (map str (cycle [(str root "/")]) (iterate inc 0)))

(defn get-tuples [sink]
  (with-open [it (.openForRead sink (JobConf.))]
       (doall (map #(Util/coerceFromTuple (Tuple. (.getTuple %))) (iterator-seq it)))))

(defstruct tap-spec :fields :tuples)

(defn mk-test-source [spec path]
  (let [source (mk-test-tap (:fields spec) path)]
    (with-open [collector (.openForWrite source (JobConf.))]
      (doall (map #(.add collector (Util/coerceToTuple %)) (:tuples spec))))
    source ))

(defn mk-test-sink [[fields-def data] path]
  [(mk-test-tap fields-def path) data])

(defn mk-test-sink [spec path]
  (mk-test-tap (:fields spec) path))

(defn test-assembly [source-specs sink-specs assembly]
    (with-log-level :warn
      (with-tmp-files [source-path (temp-dir "sources")
                       sink-path (temp-path "sinks")]
            (let
              [source-specs   (collectify source-specs)
               sink-specs     (collectify sink-specs)
               sources        (map mk-test-source source-specs (unique-rooted-paths source-path))
               sinks          (map mk-test-sink sink-specs (unique-rooted-paths sink-path))
               flow           (w/mk-flow sources sinks assembly)
               _              (w/exec flow)
               out-tuples     (doall (map get-tuples sinks))
               expected-data  (map :tuples sink-specs)]
               (is (= (map multi-set expected-data) (map multi-set out-tuples)))
               ))))
