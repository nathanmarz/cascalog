(ns cascalog.more-taps
  (:use cascalog.api)
  (:require [cascalog.cascading.tap :as tap]
            [cascalog.cascading.util :as w]
            [cascalog.logic.vars :as v])
  (:import [cascading.scheme.hadoop TextDelimited WritableSequenceFile]
           cascading.scheme.hadoop.TextLine$Compress
           [cascading.tuple Fields]
           [cascalog.moreTaps WholeFile]))

(defn- delimited
  [field-seq delim
   & {:keys [classes compress? compression skip-header? quote
             write-header? strict? safe?]
      :or {quote "\"", strict? true, safe? true}}]
  (let [[skip-header? write-header? strict? safe?]
        (map boolean [skip-header? write-header? strict? safe?])
        field-seq    (w/fields field-seq)
        field-seq    (if (and classes (not (.isDefined field-seq)))
                       (w/fields (v/gen-nullable-vars (count classes)))
                       field-seq)
        compress-setting (if compress? ;; compress? available for backwards compat
                           TextLine$Compress/ENABLE
                           (case compression
                             :enable  TextLine$Compress/ENABLE
                             :disable TextLine$Compress/DISABLE
                             TextLine$Compress/DEFAULT))]
    (if classes
      (TextDelimited. field-seq compress-setting skip-header? write-header?
                      delim strict? quote (into-array classes) safe?)
      (TextDelimited. field-seq compress-setting skip-header? write-header? delim quote))))

(defn hfs-delimited
  "
  Creates a tap on HDFS using Cascading's TextDelimited
  scheme. Different filesystems can be selected by using different
  prefixes for `path`.

  Supports TextDelimited keyword option for `:outfields`, `:classes`,
  `:skip-header?`, `:delimiter`, `:write-header?`, `:strict?`,
  `safe?`, and `:quote`.

  Also supports:
  `:compression` - one of `:enable`, `:disable` or `:default`

  See `cascalog.cascading.tap/hfs-tap` for more keyword arguments.

  See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
  http://www.cascading.org/javadoc/cascading/scheme/TextDelimited.html
  "
  [path & opts]
  (let [{:keys [outfields delimiter]} (apply array-map opts)
        scheme (apply delimited
                      (or outfields Fields/ALL)
                      (or delimiter "\t")
                      opts)]
    (apply tap/hfs-tap scheme path opts)))

(defn lfs-delimited
  "
  Creates a tap on the local filesystem using Cascading's
  TextDelimited scheme. Different filesystems can be selected by
  using different prefixes for `path`.

  Supports TextDelimited keyword option for `:outfields`, `:classes`,
  `:compress?`, `:skip-header?`, `:delimiter`, `:write-header?`,
  `:strict?`, `safe?`, and `:quote`.

  See `cascalog.cascading.tap/hfs-tap` for more keyword arguments.

  See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
  http://www.cascading.org/javadoc/cascading/scheme/TextDelimited.html
  "
  [path & opts]
  (let [{:keys [outfields delimiter]} (apply array-map opts)
        scheme (apply delimited
                      (or outfields Fields/ALL)
                      (or delimiter "\t")
                      opts)]
    (apply tap/lfs-tap scheme path opts)))


(defn writable-sequence-file [field-names key-type value-type]
  (WritableSequenceFile. (w/fields field-names) key-type value-type))

(defn hfs-wrtseqfile
  "Creates a tap on HDFS using sequence file format. Different
   filesystems can be selected by using different prefixes for `path`.

  Supports keyword option for `:outfields`. See `cascalog.tap/hfs-tap`
  for more keyword arguments.

   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path key-type value-type & opts]
  (let [scheme (-> (:outfields (apply array-map opts) Fields/ALL)
                   (writable-sequence-file key-type value-type))]
    (apply tap/hfs-tap scheme path opts)))

(defn lfs-wrtseqfile
  "Creates a tap on local file system using sequence file format. Different
   filesystems can be selected by using different prefixes for `path`.

  Supports keyword option for `:outfields`. See `cascalog.tap/hfs-tap`
  for more keyword arguments.

   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path key-type value-type & opts]
  (let [scheme (-> (:outfields (apply array-map opts) Fields/ALL)
                   (writable-sequence-file key-type value-type))]
    (apply tap/lfs-tap scheme path opts)))

(defn- whole-file
  "Custom scheme for dealing with entire files."
  [field-names]
  (WholeFile. (w/fields field-names)))

(defn hfs-wholefile
  "Subquery to return distinct files in the supplied directory. Files
  will be returned as 2-tuples, formatted as `<filename, file>` The
  filename is a text object, while the entire, unchopped file is
  encoded as a Hadoop `BytesWritable` object."
  [path & opts]
  (let [scheme (-> (:outfields (apply array-map opts) Fields/ALL)
                   (whole-file))]
    (apply hfs-tap scheme path opts)))
