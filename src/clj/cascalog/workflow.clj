;;    Copyright 2010 Nathan Marz
;; 
;;    This program is free software: you can redistribute it and/or modify
;;    it under the terms of the GNU General Public License as published by
;;    the Free Software Foundation, either version 3 of the License, or
;;    (at your option) any later version.
;; 
;;    This program is distributed in the hope that it will be useful,
;;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;    GNU General Public License for more details.
;; 
;;    You should have received a copy of the GNU General Public License
;;    along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns cascalog.workflow
  (:refer-clojure
   :exclude [group-by count first filter mapcat map identity min max])
  (:use [cascalog.debug :only (debug-print)]
        [clojure.tools.macro :only (name-with-attributes)]
        [jackknife.core :only (safe-assert)]
        [jackknife.seq :only (collectify)])
  (:require [cascalog.conf :as conf]
            [cascalog.vars :as v]
            [cascalog.util :as u]
            [hadoop-util.core :as hadoop]
            [serializable.fn :as s])
  (:import [cascalog Util]
           [java.io File]
           [java.util ArrayList]
           [cascading.tuple Tuple TupleEntry Fields]
           [cascading.scheme Scheme TextLine SequenceFile TextDelimited]
           [cascading.tap Hfs Lfs GlobHfs Tap TemplateTap SinkMode]
           [cascading.tuple TupleEntryCollector]
           [cascading.flow Flow FlowConnector]
           [cascading.cascade Cascades]
           [cascalog.ops KryoInsert]
           [cascading.operation Identity Debug]
           [cascading.operation.aggregator First Count Sum Min Max]
           [cascading.pipe Pipe Each Every GroupBy CoGroup]
           [cascading.pipe.cogroup InnerJoin OuterJoin LeftJoin RightJoin MixedJoin]
           [cascalog ClojureFilter ClojureMapcat ClojureMap
            ClojureAggregator Util ClojureBuffer ClojureBufferIter
            FastFirst MemorySourceTap MultiGroupBy ClojureMultibuffer]))

(defn fields
  {:tag Fields}
  [obj]
  (if (or (nil? obj) (instance? Fields obj))
    obj
    (let [obj (collectify obj)]
      (if (empty? obj)
        Fields/ALL ; this is a hack since cascading doesn't support selecting no fields
        (Fields. (into-array String obj))))))

(defn fields-array
  [fields-seq]
  (into-array Fields (clojure.core/map fields fields-seq)))

(defn pipes-array
  [pipes]
  (into-array Pipe pipes))

(defn- fields-obj? [obj]
  "Returns true for a Fields instance, a string, or an array of strings."
  (or
   (instance? Fields obj)
   (string? obj)
   (and (sequential? obj) (every? string? obj))))

(defn parse-args
  "arr => op in-fields? :fn> func-fields :> out-fields
  
  returns [in-fields func-fields spec out-fields]"
  ([arr] (parse-args arr Fields/RESULTS))
  ([[op & varargs] defaultout]
     (let [first-elem (clojure.core/first varargs)
           [in-fields keyargs] (if (or (nil? first-elem)
                                       (keyword? first-elem))
                                 [Fields/ALL (apply hash-map varargs)]
                                 [(fields (clojure.core/first varargs))
                                  (apply hash-map (rest varargs))])
           options  (merge {:fn> (:fields (meta op)) :> defaultout} keyargs)]
       [in-fields (fields (:fn> options)) op (fields (:> options))])))

(defn pipe
  "Returns a Pipe of the given name, or if one is not supplied with a
   unique random name."
  ([] (pipe (u/uuid)))
  ([^String name]
     (Pipe. name)))

(defn pipe-rename
  [^String name]
  (fn [p]
    (debug-print "pipe-rename" name)
    (Pipe. name p)))

(defn- as-pipes
  [pipe-or-pipes]
  (let [pipes (if (instance? Pipe pipe-or-pipes)
                [pipe-or-pipes] pipe-or-pipes)]
    (into-array Pipe pipes)))

;; with a :fn> defined, turns into a function
(defn filter [& args]
  (fn [previous]
    (debug-print "filter" args)
    (let [[in-fields func-fields op out-fields] (parse-args args)]
      (if func-fields
        (Each. previous in-fields
               (ClojureMap. func-fields op) out-fields)
        (Each. previous in-fields
               (ClojureFilter. op))))))

(defn mapcat [& args]
  (fn [previous]
    (debug-print "mapcat" args)
    (let [[in-fields func-fields op out-fields] (parse-args args)]
      (Each. previous in-fields
             (ClojureMapcat. func-fields op) out-fields))))

(defn map [& args]
  (fn [previous]
    (debug-print "map" args)
    (let [[in-fields func-fields op out-fields] (parse-args args)]
      (Each. previous in-fields
             (ClojureMap. func-fields op) out-fields))))

(defn group-by
  ([]
     (fn [& previous]
       (debug-print "groupby no grouping fields")
       (GroupBy. (as-pipes previous))))
  ([group-fields]
     (fn [& previous]
       (debug-print "groupby" group-fields)
       (GroupBy. (as-pipes previous) (fields group-fields))))
  ([group-fields sort-fields]
     (fn [& previous]
       (debug-print "groupby" group-fields sort-fields)
       (GroupBy. (as-pipes previous) (fields group-fields) (fields sort-fields))))
  ([group-fields sort-fields reverse-order]
     (fn [& previous]
       (debug-print "groupby" group-fields sort-fields reverse-order)
       (GroupBy. (as-pipes previous) (fields group-fields) (fields sort-fields) reverse-order))))

(defn count [^String count-field]
  (fn [previous]
    (debug-print "count" count-field)
    (Every. previous (Count. (fields count-field)))))

(defn sum [^String in-fields ^String sum-fields]
  (fn [previous]
    (debug-print "sum" in-fields sum-fields)
    (Every. previous (fields in-fields) (Sum. (fields sum-fields)))))

(defn min [^String in-fields ^String min-fields]
  (fn [previous]
    (debug-print "min" in-fields min-fields)
    (Every. previous (fields in-fields) (Min. (fields min-fields)))))

(defn max [^String in-fields ^String max-fields]
  (fn [previous]
    (debug-print "groupby" in-fields max-fields)
    (Every. previous (fields in-fields) (Max. (fields max-fields)))))

(defn first []
  (fn [previous]
    (debug-print "first")
    (Every. previous (First.) Fields/RESULTS)))

(defn fast-first []
  (fn [previous]
    (debug-print "fast-first")
    (Every. previous (FastFirst.) Fields/RESULTS)))

(defn select [keep-fields]
  (fn [previous]
    (debug-print "select" keep-fields)
    (let [ret (Each. previous (fields keep-fields) (Identity.))]
      ret
      )))

(defn identity [& args]
  (fn [previous]
    (debug-print "identity" args)
    ;;  + is a hack. TODO: split up parse-args into parse-args and parse-selector-args
    (let [[in-fields func-fields _ out-fields] (parse-args (cons + args) Fields/RESULTS)
          id-func (if func-fields (Identity. func-fields) (Identity.))]
      (Each. previous in-fields id-func out-fields))))

(defn pipe-name [name]
  (fn [p]
    (debug-print "pipe-name" name)
    (Pipe. name p)))

(defn insert [newfields vals]
  (fn [previous]
    (debug-print "insert" newfields vals)
    (Each. previous (KryoInsert. (fields newfields)
                                 (into-array Object (collectify vals)))
           Fields/ALL)))

(defn raw-each
  ([arg1] (fn [p] (debug-print "raw-each" arg1) (Each. p arg1)))
  ([arg1 arg2] (fn [p] (debug-print "raw-each" arg1 arg2) (Each. p arg1 arg2)))
  ([arg1 arg2 arg3] (fn [p]
                      (debug-print "raw-each" arg1 arg2 arg3)
                      (Each. p arg1 arg2 arg3))))

(defn debug []
  (raw-each (Debug. true)))

(defn raw-every
  ([arg1] (fn [p] (debug-print "raw-every" arg1) (Every. p arg1)))
  ([arg1 arg2] (fn [p] (debug-print "raw-every" arg1 arg2) (Every. p arg1 arg2)))
  ([arg1 arg2 arg3] (fn [p]
                      (debug-print "raw-every" arg1 arg2 arg3)
                      (Every. p arg1 arg2 arg3))))

(defn aggregate [& args]
  (fn [^Pipe previous]
    (debug-print "aggregate" args)
    (let [[^Fields in-fields func-fields op ^Fields out-fields]
          (parse-args args Fields/ALL)]
      (Every. previous in-fields
              (ClojureAggregator. func-fields op) out-fields))))

(defn buffer [& args]
  (fn [^Pipe previous]
    (debug-print "buffer" args)
    (let [[^Fields in-fields func-fields op ^Fields out-fields]
          (parse-args args Fields/ALL)]
      (Every. previous in-fields
              (ClojureBuffer. func-fields op) out-fields))))

(defn bufferiter [& args]
  (fn [^Pipe previous]
    (debug-print "bufferiter" args)
    (let [[^Fields in-fields func-fields op ^Fields out-fields] (parse-args args Fields/ALL)]
      (Every. previous in-fields
              (ClojureBufferIter. func-fields op) out-fields))))

(defn multibuffer [& args]
  (fn [pipes fields-sum]
    (debug-print "multibuffer" args)
    (let [[group-fields func-fields op _] (parse-args args Fields/ALL)]
      (MultiGroupBy.
       pipes
       group-fields
       fields-sum
       (ClojureMultibuffer. func-fields op)))))

;; we shouldn't need a seq for fields (b/c we know how many pipes we have)
(defn co-group
  [fields-seq declared-fields joiner]
  (fn [& pipes-seq]
    (debug-print "cogroup" fields-seq declared-fields joiner)
    (CoGroup.
  	  (pipes-array pipes-seq)
  	  (fields-array fields-seq)
  	  (fields declared-fields)
  	  joiner)))

(defn mixed-joiner [bool-seq]
  (MixedJoin. (boolean-array bool-seq)))

(defn outer-joiner [] (OuterJoin.))

(defn ophelper [type builder body]
  `(merge-meta (s/fn ~@body) {::op-builder ~builder :pred-type type}))

(defmacro mapop [& body] (ophelper :map map body))
(defmacro mapcatop [& body] (ophelper :mapcat mapcat body))
(defmacro filterop [& body] (ophelper :filter filter body))
(defmacro aggregateop [& body] (ophelper :aggregate aggregate body))
(defmacro bufferop [& body] (ophelper :buffer buffer body))
(defmacro bufferiterop [& body] (ophelper :bufferiter bufferiter body))
(defmacro multibufferop [& body] (ophelper :multibuffer multibuffer body))

;; TODO: need to add ability to do metadata args and such for docstrings...
;; TODO: need to add ability to make a "prepared op" that will be called with the flowProcess, etc. in the task.
(defn defhelper [name op-sym body]
  `(def ~name (~op-sym ~@body)))

(defmacro defmapop [name & body] (defhelper name 'mapop body))
(defmacro defmapcatop [name & body] (defhelper name 'mapcatop body))
(defmacro deffilterop [name & body] (defhelper name 'filterop body))
(defmacro defaggregateop [name & body] (defhelper name 'aggregateop body))
(defmacro defbufferop [name & body] (defhelper name 'bufferop body))
(defmacro defbufferiterop [name & body] (defhelper name 'bufferiterop body))
(defmacro defmultibufferop [name & body] (defhelper name 'multibufferop body))

(defn exec [op & args]
  (let [builder (get (meta op) ::op-builder filter)]
    (apply builder op args)
    ))

(defn assemble
  ([x] x)
  ([x form] (apply form (collectify x)))
  ([x form & more] (apply assemble (assemble x form) more)))

(defmacro assembly
  ([args return]
     `(assembly ~args [] ~return))
  ([args bindings return]
     (let [pipify (fn [forms] (if (or (not (sequential? forms))
                                      (vector? forms))
                                forms
                                (cons 'cascalog.workflow/assemble forms)))
           return (pipify return)
           bindings (vec (clojure.core/map #(%1 %2) (cycle [clojure.core/identity pipify]) bindings))]
       `(fn ~args
          (let ~bindings
            ~return)))))

(defmacro defassembly
  ([name args return]
     `(defassembly ~name ~args [] ~return))
  ([name args bindings return]
     `(def ~name (cascalog.workflow/assembly ~args ~bindings ~return))))

(defn join-assembly [fields-seq declared-fields joiner]
  (assembly [& pipes-seq]
            (pipes-seq (co-group fields-seq declared-fields joiner))))

(defn inner-join [fields-seq declared-fields]
  (join-assembly fields-seq declared-fields (InnerJoin.)))

(defn outer-join [fields-seq declared-fields]
  (join-assembly fields-seq declared-fields (OuterJoin.)))

(defn taps-map [pipes taps]
  (Cascades/tapsMap (into-array Pipe pipes) (into-array Tap taps)))

(defn mk-flow [sources sinks assembly]
  (let [sources (collectify sources)
        sinks   (collectify sinks)
        source-pipes (clojure.core/map #(Pipe. (str "spipe" %2))
                                       sources
                                       (iterate inc 0))
        tail-pipes (clojure.core/map #(Pipe. (str "tpipe" %2) %1)
                                     (collectify (apply assembly source-pipes))
                                     (iterate inc 0))]
    (.connect (FlowConnector.)
              (taps-map source-pipes sources)
              (taps-map tail-pipes sinks)
              (into-array Pipe tail-pipes))))

(defn text-line
  ([]
     (TextLine.))
  ([field-names]
     (TextLine. (fields field-names) (fields field-names)))
  ([source-fields sink-fields]
     (TextLine. (fields source-fields) (fields sink-fields))))

(defn sequence-file [field-names]
  (SequenceFile. (fields field-names)))

(defn delimited
  [field-seq delim & {:keys [classes skip-header?]}]
  (let [skip-header? (boolean skip-header?)
        field-seq    (fields field-seq)
        field-seq    (if (and classes (not (.isDefined field-seq)))
                       (fields (v/gen-nullable-vars (count classes)))
                       field-seq)]
    (if classes
      (TextDelimited. field-seq skip-header? delim (into-array classes))
      (TextDelimited. field-seq skip-header? delim))))

(deffilterop equal [& objs]
  (apply = objs))

(defn compose-straight-assemblies [& all]
  (fn [input]
    (apply assemble input all)))

(defn path
  {:tag String}
  [x]
  (if (string? x) x (.getAbsolutePath ^File x)))

(def valid-sinkmode? #{:keep :append :replace})

(defn- sink-mode [kwd]
  {:pre [(or (nil? kwd) (valid-sinkmode? kwd))]}
  (case kwd
    :keep SinkMode/KEEP
    :append SinkMode/APPEND
    :replace SinkMode/REPLACE
    SinkMode/KEEP))

(defn set-sinkparts!
  "If `sinkparts` is truthy, returns the supplied cascading scheme
with the `sinkparts` field updated appropriately; else, acts as
identity.  identity."
  [^Scheme scheme sinkparts]
  (if sinkparts
    (doto scheme (.setNumSinkParts sinkparts))
    scheme))

(defn hfs
  ([scheme path-or-file]
     (Hfs. scheme (path path-or-file)))
  ([^Scheme scheme path-or-file sinkmode]
     (Hfs. scheme
           (path path-or-file)
           (sink-mode sinkmode))))

(defn lfs
  ([scheme path-or-file]
     (Lfs. scheme (path path-or-file)))
  ([^Scheme scheme path-or-file sinkmode]
     (Lfs. scheme
           (path path-or-file)
           (sink-mode sinkmode))))

(defn glob-hfs [^Scheme scheme path-or-file source-pattern]
  (GlobHfs. scheme (str (path path-or-file)
                        source-pattern)))

(defn template-tap
  ([^Hfs parent sink-template]
     (TemplateTap. parent sink-template))
  ([^Hfs parent sink-template templatefields]
     (TemplateTap. parent
                   sink-template
                   (fields templatefields))))

(defn write-dot [^Flow flow ^String path]
  (.writeDOT flow path))

(defn fill-tap! [^Tap tap xs] 
  (with-open [^TupleEntryCollector collector (.openForWrite tap (hadoop/job-conf
                                                                 (conf/project-conf)))]
    (doseq [item xs]
      (.add collector (Util/coerceToTuple item)))))

(defn memory-source-tap
  ([tuples] (memory-source-tap Fields/ALL tuples))
  ([fields-in tuples]
     (let [tuples (->> tuples
                       (clojure.core/map #(Util/coerceToTuple %))
                       (ArrayList.))]
       (MemorySourceTap. tuples (fields fields-in)))))
