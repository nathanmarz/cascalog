(ns cascalog.fluent.workflow
  (:refer-clojure
   :exclude [group-by count first filter mapcat map identity min max])
  (:use [cascalog.debug :only (debug-print)]
        [clojure.tools.macro :only (name-with-attributes)]
        [jackknife.core :only (safe-assert)]
        [jackknife.seq :only (collectify)]
        [jackknife.def :only (defalias)])
  (:require [cascalog.fluent.conf :as conf]
            [cascalog.fluent.tap :as tap]
            [cascalog.fluent.cascading :as cascading]
            [cascalog.fluent.def :as d]
            [cascalog.fluent.flow :as flow]
            [cascalog.util :as u]
            [hadoop-util.core :as hadoop])
  (:import [cascalog Util]
           [java.io File]
           [java.util ArrayList]
           [cascading.tuple Tuple TupleEntry Fields]
           [cascading.scheme.hadoop TextLine SequenceFile TextDelimited]
           [cascading.scheme Scheme]
           [cascading.tap Tap SinkMode]
           [cascading.tap.hadoop Hfs Lfs GlobHfs TemplateTap]
           [cascading.tuple TupleEntryCollector]
           [cascading.flow Flow  FlowDef]
           [cascading.flow.hadoop HadoopFlowProcess HadoopFlowConnector]
           [cascading.cascade Cascades]
           [cascalog.ops KryoInsert]
           [cascading.operation Identity Debug]
           [cascading.operation.aggregator First Count Sum Min Max]
           [cascading.pipe Pipe Each Every GroupBy CoGroup]
           [cascading.pipe.joiner InnerJoin]
           [com.twitter.maple.tap MemorySourceTap]
           [cascalog ClojureFilter ClojureMapcat ClojureMap
            ClojureAggregator Util ClojureBuffer ClojureBufferIter
            FastFirst MultiGroupBy ClojureMultibuffer]))

(defalias fields-array cascading/fields-array)
(defalias fields cascading/fields)
(defalias pipes-array cascading/pipes-array)
(defalias taps-array cascading/taps-array)

(defn parse-args
  "arr => func-spec in-fields? :fn> func-fields :> out-fields

  returns [in-fields func-fields spec out-fields]"
  ([arr] (parse-args arr Fields/RESULTS))
  ([[func-args & varargs] defaultout]
     (let [spec      (fn-spec func-args)
           func-var  (if (var? func-args)
                       func-args
                       (clojure.core/first func-args))
           first-elem (clojure.core/first varargs)
           [in-fields keyargs] (if (or (nil? first-elem)
                                       (keyword? first-elem))
                                 [Fields/ALL (apply hash-map varargs)]
                                 [(fields (clojure.core/first varargs))
                                  (apply hash-map (rest varargs))])
           stateful (get (meta func-var) :stateful false)
           options  (merge {:fn> (:fields (meta func-var)) :> defaultout} keyargs)]
       [in-fields (fields (:fn> options)) spec (fields (:> options)) stateful])))

;; ## Pipe Operations

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

;; with a :fn> defined, turns into a function
(defn filter [& args]
  (fn [previous]
    (debug-print "filter" args)
    (let [[in-fields func-fields spec out-fields stateful] (parse-args args)]
      (if func-fields
        (Each. previous in-fields
               (ClojureMap. func-fields spec stateful) out-fields)
        (Each. previous in-fields
               (ClojureFilter. spec stateful))))))

(defn mapcat [& args]
  (fn [previous]
    (debug-print "mapcat" args)
    (let [[in-fields func-fields spec out-fields stateful] (parse-args args)]
      (Each. previous in-fields
             (ClojureMapcat. func-fields spec stateful) out-fields))))

(defn map [& args]
  (fn [previous]
    (debug-print "map" args)
    (let [[in-fields func-fields spec out-fields stateful] (parse-args args)]
      (Each. previous in-fields
             (ClojureMap. func-fields spec stateful)
             out-fields))))

(letfn [(as-pipes [pipe-or-pipes]
          (pipes-array (if (instance? Pipe pipe-or-pipes)
                         [pipe-or-pipes]
                         pipe-or-pipes)))]
  (defn group-by
    ([]
       (fn [& previous]
         (debug-print "groupby no grouping fields")
         (GroupBy. (as-pipes previous))))
    ([group-fields]
       (fn [& previous]
         (debug-print "groupby" group-fields)
         (GroupBy. (as-pipes previous)
                   (fields group-fields))))
    ([group-fields sort-fields]
       (fn [& previous]
         (debug-print "groupby" group-fields sort-fields)
         (GroupBy. (as-pipes previous)
                   (fields group-fields)
                   (fields sort-fields))))
    ([group-fields sort-fields reverse-order]
       (fn [& previous]
         (debug-print "groupby" group-fields sort-fields reverse-order)
         (GroupBy. (as-pipes previous)
                   (fields group-fields)
                   (fields sort-fields)
                   reverse-order)))))

(defn count [^String count-field]
  (fn [previous]
    (debug-print "count" count-field)
    (Every. previous (Count. (fields count-field)))))

(defn first []
  (fn [previous]
    (debug-print "first")
    (Every. previous
            (First.)
            Fields/RESULTS)))

;; TODO: consider using
;; "http://docs.cascading.org/cascading/1.2/javadoc/cascading/pipe/assembly/Unique.html"
(defn fast-first []
  (fn [previous]
    (debug-print "fast-first")
    (Every. previous
            (FastFirst.)
            Fields/RESULTS)))

(defn select [keep-fields]
  (fn [previous]
    (debug-print "select" keep-fields)
    (Each. previous
           (fields keep-fields)
           (Identity.))))

(defn identity [& args]
  (fn [previous]
    (debug-print "identity" args)
    ;;  + is a hack. TODO: split up parse-args into parse-args and
    ;;  parse-selector-args
    (let [[in-fields func-fields _ out-fields _] (parse-args (cons #'+ args)
                                                             Fields/RESULTS)
          id-func (if func-fields
                    (Identity. func-fields)
                    (Identity.))]
      (Each. previous in-fields id-func out-fields))))

(defn insert [newfields vals]
  (fn [previous]
    (debug-print "insert" newfields vals)
    (Each. previous (KryoInsert. (fields newfields)
                                 (into-array Object (collectify vals)))
           Fields/ALL)))

(defn raw-each
  ([arg1]
     (fn [p]
       (debug-print "raw-each" arg1)
       (Each. p arg1)))
  ([arg1 arg2]
     (fn [p]
       (debug-print "raw-each" arg1 arg2)
       (Each. p arg1 arg2)))
  ([arg1 arg2 arg3]
     (fn [p]
       (debug-print "raw-each" arg1 arg2 arg3)
       (Each. p arg1 arg2 arg3))))

(defn raw-every
  ([arg1] (fn [p] (debug-print "raw-every" arg1) (Every. p arg1)))
  ([arg1 arg2] (fn [p] (debug-print "raw-every" arg1 arg2) (Every. p arg1 arg2)))
  ([arg1 arg2 arg3] (fn [p]
                      (debug-print "raw-every" arg1 arg2 arg3)
                      (Every. p arg1 arg2 arg3))))

(defn aggregate [& args]
  (fn [^Pipe previous]
    (debug-print "aggregate" args)
    (let [[^Fields in-fields func-fields specs ^Fields out-fields stateful]
          (parse-args args Fields/ALL)]
      (Every. previous in-fields
              (ClojureAggregator. func-fields specs stateful)
              out-fields))))

(defn buffer [& args]
  (fn [^Pipe previous]
    (debug-print "buffer" args)
    (let [[^Fields in-fields func-fields specs ^Fields out-fields stateful]
          (parse-args args Fields/ALL)]
      (Every. previous in-fields
              (ClojureBuffer. func-fields specs stateful) out-fields))))

(defn bufferiter [& args]
  (fn [^Pipe previous]
    (debug-print "bufferiter" args)
    (let [[^Fields in-fields func-fields specs ^Fields out-fields stateful]
          (parse-args args Fields/ALL)]
      (Every. previous in-fields
              (ClojureBufferIter. func-fields specs stateful) out-fields))))

(defn multibuffer [& args]
  (fn [pipes fields-sum]
    (debug-print "multibuffer" args)
    (let [[group-fields func-fields specs _ stateful] (parse-args args Fields/ALL)]
      (MultiGroupBy. pipes
                     group-fields
                     fields-sum
                     (ClojureMultibuffer. func-fields specs stateful)))))

;; we shouldn't need a seq for fields (b/c we know how many pipes we
;; have)

(defn co-group
  [fields-seq declared-fields joiner]
  (fn [& pipes-seq]
    (debug-print "cogroup" fields-seq declared-fields joiner)
    (CoGroup. (pipes-array pipes-seq)
              (fields-array fields-seq)
              (fields declared-fields)
              joiner)))

;; ## Operations End Here.

(defn assemble
  ([x] x)
  ([x form] (apply form (collectify x)))
  ([x form & more] (apply assemble (assemble x form) more)))

(defn taps-map
  "Returns a map of pipe name to tap."
  [pipes taps]
  (Cascades/tapsMap (pipes-array pipes)
                    (taps-array taps)))

(defn mk-flow
  "Connects a number of sources and sinks to an assembly. The sources
  and sinks are supplied as a sequence; " [sources sinks assembly]
  (let [sources (collectify sources)
        sinks   (collectify sinks)
        source-pipes (clojure.core/map #(pipe (str "spipe" %2))
                                       sources
                                       (iterate inc 0))
        _ (prn "SOURCE: "(taps-map source-pipes sources))
        _ (prn "ASSEMBLY: " assembly)
        tail-pipes (clojure.core/map #(Pipe. (str "tpipe" %2) %1)
                                     (collectify (apply assembly source-pipes))
                                     (iterate inc 0))
        _ (prn "TAIL PIPES " tail-pipes)
        _ (prn "APPLIED " (apply assembly source-pipes))]
    (.connect (HadoopFlowConnector.)
              (taps-map source-pipes sources)
              (taps-map tail-pipes sinks)
              (pipes-array tail-pipes))))

;; Source

(defalias defmapop d/defmapop)
(defalias defmapcatop d/defmapcatop)
(defalias deffilterop d/deffilterop)
(defalias defaggregateop d/defaggregateop)
(defalias defbufferop d/defbufferop)
(defalias defmultibufferop d/defmultibufferop)
(defalias defbufferiterop d/defbufferiterop)
(defalias path u/path)
(defalias flow-def flow/flow-def)
(defalias sequence-file tap/sequence-file)
(defalias text-line tap/text-line)
(defalias fill-tap! tap/fill-tap!)
(defalias set-sinkparts! tap/set-sinkparts!)
(defalias valid-sinkmode? tap/valid-sinkmode?)
(defalias memory-source-tap tap/memory-source-tap)
(defalias template-tap tap/template-tap)
(defalias glob-hfs tap/glob-hfs)
(defalias lfs tap/lfs)
(defalias hfs tap/hfs)

(deffilterop equal [& objs]
  (apply = objs))

(defn compose-straight-assemblies [& all]
  (fn [input]
    (apply assemble input all)))
