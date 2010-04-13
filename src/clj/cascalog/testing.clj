 ;    Copyright 2010 Nathan Marz
 ; 
 ;    This program is free software: you can redistribute it and/or modify
 ;    it under the terms of the GNU General Public License as published by
 ;    the Free Software Foundation, either version 3 of the License, or
 ;    (at your option) any later version.
 ; 
 ;    This program is distributed in the hope that it will be useful,
 ;    but WITHOUT ANY WARRANTY; without even the implied warranty of
 ;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 ;    GNU General Public License for more details.
 ; 
 ;    You should have received a copy of the GNU General Public License
 ;    along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns cascalog.testing
  (:use clojure.test
        clojure.contrib.java-utils
        cascalog.io
        cascalog.util
        cascalog.api)
  (:import [cascading.tuple Fields Tuple TupleEntry TupleEntryCollector]
           [cascading.pipe Pipe]
           [cascading.operation ConcreteCall]
           [cascading.flow FlowProcess]
           [cascalog Util ClojureMap MemorySourceTap]
           [java.lang Comparable]
           [java.util ArrayList]
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
  (map str (cycle [(str root "/")]) (repeatedly uuid)))

(defn get-tuples [sink]
  (with-open [it (.openForRead sink (JobConf.))]
       (doall (map #(vec (Util/coerceFromTuple (Tuple. (.getTuple %)))) (iterator-seq it)))))

(defn- gen-fake-fields [amt]
  (take amt (map str (iterate inc 1))))

(defn- mapify-spec [spec]
  (if (map? spec)
    spec
    {:fields Fields/ALL :tuples spec} ))

(defn mk-test-source [spec path]
  (let [spec (mapify-spec spec)
        source (mk-test-tap (:fields spec) path)]
    ;; unable to use with-log-level here for some reason
    (with-open [collector (.openForWrite source (JobConf.))]
      (doall (map #(.add collector (Util/coerceToTuple %)) (:tuples spec))))
    source ))

(defn mk-test-sink [spec path]
  (mk-test-tap (:fields (mapify-spec spec)) path))

(defn test-assembly
  ([source-specs sink-specs assembly]
    (test-assembly :fatal source-specs sink-specs assembly))
  ([log-level source-specs sink-specs assembly]
    (with-log-level log-level
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
               )))))

(defn- mk-tmpfiles+forms [amt]
  (let [tmpfiles  (take amt (repeatedly (fn [] (gensym "tap"))))
        tmpforms  (vec (mapcat (fn [f] [f `(cascalog.io/temp-dir ~(str f))]) tmpfiles))]
    [tmpfiles tmpforms]
  ))

;; bindings are name {:fields :tuples}
(defmacro with-tmp-sources [bindings & body]
  (let [parts     (partition 2 bindings)
        names     (map first parts)
        specs     (map second parts)
        [tmpfiles tmpforms] (mk-tmpfiles+forms (count parts))
        tmptaps   (vec (mapcat (fn [n t s] [n `(cascalog.testing/mk-test-source ~s ~t)])
                    names tmpfiles specs))]
        `(cascalog.io/with-tmp-files ~tmpforms
           (let ~tmptaps
              ~@body
            ))))

(defn- doublify [tuples]
  (for [t tuples]
  (map (fn [v] (if (number? v) (double v) v)) t)))

(defn test?- [& bindings]
  (let [[log-level bindings] (if (keyword? (first bindings))
                                [(first bindings) (rest bindings)]
                                [:fatal bindings])]
    (with-log-level log-level
      (with-tmp-files [sink-path (temp-dir "sink")]
        (let [[specs rules]  (unweave bindings)
              sinks          (map mk-test-sink specs (unique-rooted-paths sink-path))
              _              (apply ?- (interleave sinks rules))
              out-tuples     (doall (map get-tuples sinks))
              spec-sets      (map multi-set (map doublify specs))
              out-sets       (map multi-set (map doublify out-tuples))]
              (is (= spec-sets out-sets)))))))

(defmacro test?<- [& args]
  (let [[begin body] (if (keyword? (first args))
                      (split-at 2 args)
                      (split-at 1 args))]
  `(test?- ~@begin (<- ~@body))))

(defn memory-source-tap [tuples]
  (let [tuples (ArrayList. (map #(Util/coerceToTuple %) tuples))]
    (MemorySourceTap. tuples)))
