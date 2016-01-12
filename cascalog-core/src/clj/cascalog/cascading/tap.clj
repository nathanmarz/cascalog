(ns cascalog.cascading.tap
  (:require [hadoop-util.core :as hadoop]
            [jackknife.core :refer (throw-illegal path)]
            [cascalog.cascading.util :refer (fields)]
            [cascalog.cascading.conf :as conf])
  (:import [java.util ArrayList]
           [cascalog Util]
           [cascading.tap Tap SinkMode]
           [cascading.tap.hadoop Hfs Lfs GlobHfs TemplateTap]
           [cascading.tuple TupleEntryCollector TupleEntryIterator]
           [cascading.scheme Scheme]
           [cascading.scheme.hadoop TextLine TextLine$Compress SequenceFile TextDelimited]
           [cascading.flow.hadoop HadoopFlowProcess]
           [cascading.tuple Fields Tuple TupleEntry]
           [com.twitter.maple.tap StdoutTap MemorySourceTap]))

;; source can be a cascalog-tap, subquery, or cascading tap sink can
;; be a cascading tap, a sink function, or a cascalog-tap

;; Pairing of source and sink.

;; TODO: Plug in support here.
(defrecord CascalogTap [source sink])

(defn cascalog-tap [source sink]
  (CascalogTap. source sink))

(def valid-sinkmode? #{:keep :update :replace})

(defn- sink-mode [kwd]
  {:pre [(or (nil? kwd) (valid-sinkmode? kwd))]}
  (case kwd
    :keep    SinkMode/KEEP
    :update  SinkMode/UPDATE
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

(defn sequence-file [field-names]
  (SequenceFile. (fields field-names)))

(defn text-line
  ([] (TextLine.))
  ([field-names]
     (TextLine. (fields field-names) (fields field-names)))
  ([source-fields sink-fields]
     (TextLine. (fields source-fields) (fields sink-fields)))
  ([source-fields sink-fields compression]
     (TextLine. (fields source-fields) (fields sink-fields) compression)))

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

(defn glob-hfs
  [^Scheme scheme path-or-file source-pattern]
  (GlobHfs. scheme (str (path path-or-file)
                        source-pattern)))

(defn template-tap
  ([^Hfs parent sink-template]
     (TemplateTap. parent sink-template))
  ([^Hfs parent sink-template {:keys [templatefields open-threshold]
                               :or {templatefields Fields/ALL
                                    open-threshold 300}}]
     (TemplateTap. parent sink-template (fields templatefields) open-threshold)))

(defn- patternize
  "If `pattern` is truthy, returns the supplied parent `Hfs` or `Lfs`
  tap wrapped that responds as a `TemplateTap` when used as a sink,
  and a `GlobHfs` tap when used as a source. Otherwise, acts as
  identity."
  [scheme type path-or-file sinkmode sink-template source-pattern options]
  (let [tap-maker ({:hfs hfs :lfs lfs} type)
        parent (tap-maker scheme path-or-file sinkmode)
        source (if source-pattern
                 (glob-hfs scheme path-or-file source-pattern)
                 parent)
        sink (if sink-template
               (template-tap parent sink-template options)
               parent)]
    (CascalogTap. source sink)))

(defn hfs-tap
  "Returns a Cascading Hfs tap with support for the supplied scheme,
  opened up on the supplied path or file object. Supported keyword
  options are:

  `:sinkmode` - can be `:keep`, `:update` or `:replace`.

  `:sinkparts` - used to constrain the segmentation of output files.

  `:source-pattern` - Causes resulting tap to respond as a GlobHfs tap
  when used as source.

  `:sink-template` - Causes resulting tap to respond as a TemplateTap when
  used as a sink.

  `:templatefields` - When pattern is supplied via :sink-template,
  this option allows a subset of output fields to be used in the
  naming scheme.

  See f.ex. the
  http://docs.cascading.org/cascading/2.0/javadoc/cascading/scheme/local/TextDelimited.html
  scheme."  
  [^Scheme scheme path-or-file & {:keys [sinkmode sinkparts sink-template 
                                         source-pattern templatefields]
                                  :as options}]
  (-> scheme
      (set-sinkparts! sinkparts)
      (patternize :hfs path-or-file sinkmode
                  sink-template source-pattern options)))

(defn lfs-tap
  "Returns a Cascading Lfs tap with support for the supplied scheme,
  opened up on the supplied path or file object. Supported keyword
  options are:

  `:sinkmode` - can be `:keep`, `:update` or `:replace`.

  `:sinkparts` - used to constrain the segmentation of output files.

  `:source-pattern` - Causes resulting tap to respond as a GlobHfs tap
  when used as source.

  `:sink-template` - Causes resulting tap to respond as a TemplateTap
  when used as a sink.

  `:templatefields` - When pattern is supplied via :sink-template,
  this option allows a subset of output fields to be used in the
  naming scheme."

  [scheme path-or-file & {:keys [sinkmode sinkparts sink-template
                                 source-pattern templatefields] 
                          :as options}]
  (-> scheme
      (set-sinkparts! sinkparts)
      (patternize :lfs path-or-file sinkmode
                  sink-template source-pattern options)))

(defn hfs-textline
  "Creates a tap on HDFS using textline format. Different filesystems
   can be selected by using different prefixes for `path`. Supported
   keyword options are:

   `:outfields` - used to select the fields written to the tap

   `:compression` - one of `:enable`, `:disable` or `:default`

   See `cascalog.cascading.tap/hfs-tap` for more keyword arguments.

   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/TextLine.html"
  [path & opts]
  (let [opts-map (apply array-map opts)
        scheme (text-line ["line"]
                          (:outfields opts-map Fields/ALL)
                          (case (:compression opts-map)
                            :enable TextLine$Compress/ENABLE
                            :disable TextLine$Compress/DISABLE
                            TextLine$Compress/DEFAULT))]
    (apply hfs-tap scheme path opts)))

(defn lfs-textline
  "Creates a tap on the local filesystem using textline format.

  Supports keyword option for `:outfields`. See
  `cascalog.cascading.tap/lfs-tap` for more keyword arguments.

   See http://www.cascading.org/javadoc/cascading/tap/Lfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/TextLine.html"
  [path & opts]
  (let [scheme (->> (:outfields (apply array-map opts) Fields/ALL)
                    (text-line ["line"]))]
    (apply lfs-tap scheme path opts)))

(defn hfs-seqfile
  "Creates a tap on HDFS using sequence file format. Different
   filesystems can be selected by using different prefixes for `path`.

  Supports keyword option for `:outfields`. See
  `cascalog.cascading.tap/hfs-tap` for more keyword arguments.

   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path & opts]
  (let [scheme (-> (:outfields (apply array-map opts) Fields/ALL)
                   (sequence-file))]
    (apply hfs-tap scheme path opts)))

(defn lfs-seqfile
  "Creates a tap that reads data off of the local filesystem in
   sequence file format.

  Supports keyword option for `:outfields`. See
  `cascalog.cascading.tap/lfs-tap` for more keyword arguments.

   See http://www.cascading.org/javadoc/cascading/tap/Lfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path & opts]
  (let [scheme (-> (:outfields (apply array-map opts) Fields/ALL)
                   (sequence-file))]
    (apply lfs-tap scheme path opts)))

(defn stdout
  "Creates a tap that prints tuples sunk to it to standard
   output. Useful for experimentation in the REPL."
  [] (StdoutTap.))

(defn memory-source-tap
  ([tuples] (memory-source-tap Fields/ALL tuples))
  ([fields-in tuples]
     (let [tuples (->> tuples
                       (map #(Util/coerceToTuple %))
                       (ArrayList.))]
       (MemorySourceTap. tuples (fields fields-in)))))

;; ## Tap Helpers
(defn iter-seq [^TupleEntryIterator iter f]
  (if (.hasNext iter)
    (lazy-seq
      (cons (f (.next iter))
            (iter-seq iter f)))))

(defn pluck-tuple [^Tap tap]
  (with-open [it (-> (HadoopFlowProcess. (hadoop/job-conf (conf/project-conf)))
                     (.openTapForRead tap))]
    (if-let [iter (iter-seq it #(.getTupleCopy %))]
      (-> iter first Tuple. Util/coerceFromTuple vec)
      (throw-illegal "Cascading tap is empty -- tap must contain tuples."))))

(defn get-sink-tuples [^Tap sink]
  (let [conf (hadoop/job-conf (conf/project-conf))]
    (cond (map? sink) (get-sink-tuples (:sink sink))
          (not (.resourceExists sink conf)) []
          :else (with-open [it (-> (HadoopFlowProcess. conf)
                                   (.openTapForRead sink))]
                  (doall
                   (for [^Tuple t (iter-seq it #(.getTupleCopy %))]
                     (into [] (Tuple. t))))))))

(defn fill-tap! [^Tap tap xs]
  (with-open [^TupleEntryCollector collector
              (-> (hadoop/job-conf (conf/project-conf))
                  (HadoopFlowProcess.)
                  (.openTapForWrite tap))]
    (doseq [item xs]
      (.add collector (Util/coerceToTuple item)))))
