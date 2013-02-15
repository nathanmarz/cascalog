(ns cascalog.more-taps
  (:use cascalog.api)
  (:require [cascalog.tap :as tap]
            [cascalog.vars :as v]
            [cascalog.workflow :as w])
  (:import [cascading.scheme.hadoop TextDelimited WritableSequenceFile]
           [cascading.tuple Fields]))

(defn- delimited
  [field-seq delim & {:keys [classes skip-header? quote]}]
  (let [skip-header? (boolean skip-header?)
        field-seq    (w/fields field-seq)
        field-seq    (if (and classes (not (.isDefined field-seq)))
                       (w/fields (v/gen-nullable-vars (count classes)))
                       field-seq)]
    (if classes
      (TextDelimited. field-seq skip-header? delim quote (into-array classes))
      (TextDelimited. field-seq skip-header? delim quote))))

(defn hfs-delimited
  "
  Creates a tap on HDFS using Cascading's TextDelimited
  scheme. Different filesystems can be selected by using different
  prefixes for `path`.

  Supports TextDelimited keyword option for `:outfields`, `:classes`,
  `:skip-header?`, `:delimiter`, and `:quote`.
  See `cascalog.tap/hfs-tap` for more keyword arguments.

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
  `:skip-header?`, `:delimiter`, and `:quote`.
  See `cascalog.tap/hfs-tap` for more keyword arguments.

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
