(ns cascalog.cascading.io
  "TODO: Some of these things need to move into hadoop-util."
  (:require [clojure.java.io :as io]
            [hadoop-util.core :as hadoop]
            [cascalog.cascading.conf :as conf]
            [jackknife.core :refer (uuid)]
            [schema.core :as s])
  (:import [java.io File PrintWriter]
           [org.apache.log4j Logger Level]
           [org.apache.hadoop.io BytesWritable]))

;; While a BytesWritable object wraps a byte array, not all of the
;; bytes returned by the getBytes method are valid. As mentioned in
;; the [documentation](http://goo.gl/3qzyc), "The data is only valid
;; between 0 and getLength() - 1."

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object. As
  mentioned in the [BytesWritable javadoc](http://goo.gl/cjjlD), only
  the first N bytes are valid, where N = `(.getLength byteswritable)`."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))

(defn write-lines
  "Writes lines (a seq) to f, separated by newlines.  f is opened with
  writer, and automatically closed at the end of the sequence."
  [f lines]
  (with-open [^PrintWriter writer (io/writer f)]
    (loop [lines lines]
      (when-let [line (first lines)]
        (.write writer (str line))
        (.println writer)
        (recur (rest lines))))))

(defn delete-file-recursively
  "Delete file f. If it's a directory, recursively delete all its contents.
Raise an exception if any deletion fails unless silently is true."
  [f & [silently]]
  (let [f (io/file f)]
    (if (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-file-recursively child silently)))
    (io/delete-file f silently)))

(defn temp-path [sub-path]
  (io/file (System/getProperty "java.io.tmpdir") sub-path))

(defn temp-dir
  "1) creates a directory in System.getProperty(\"java.io.tmpdir\")
   2) calls tempDir.deleteOn Exit() so the file is deleted by the jvm.
   reference: ;http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4735419
   deleteOnExit is last resort cleanup on jvm exit."
  [sub-path]
  (let [tmp-dir (temp-path sub-path)]
    (or (.exists tmp-dir) (.mkdir tmp-dir))
    (.deleteOnExit tmp-dir)
    tmp-dir))

(s/defn unique-tmp-file :- File
  [s :- s/Str]
  (File. (str (temp-dir s) "/" (uuid))))

(defn delete-all
  "delete-file-recursively is preemptive delete on exiting the code
   block for repl and tests run in the same process."
  [bindings]
  (doseq [file (reverse (map second (partition 2 bindings)))]
    (if (.exists file)
      (delete-file-recursively file))))

(defmacro with-tmp-files [bindings & body]
  `(let ~bindings
     (try ~@body
          (finally (delete-all ~bindings)))))

(def log-levels
  {:fatal Level/FATAL
   :warn  Level/WARN
   :info  Level/INFO
   :debug Level/DEBUG
   :off   Level/OFF})

(defmacro with-log-level [level & body]
  `(let [with-lev#  (log-levels ~level)
         logger#    (Logger/getRootLogger)
         prev-lev#  (.getLevel logger#)]
     (try
       (.setLevel logger# with-lev#)
       ~@body
       (finally
        (.setLevel logger# prev-lev#)))))

(defn delete-all-fs [fs paths]
  (dorun
   (for [t paths]
     (.delete fs (hadoop/path t) true))))

(def tmp-dir-property
  "Use this variable as key in JobConf if you want to override the
  root of temporary paths. See with-fs-tmp."
  "cascalog.tmpdir")

(defmacro with-fs-tmp
  "Generates unique, temporary path names as subfolders of <root>/cascalog_reserved.
  <root> by default will be '/tmp', but you can configure it via the
  JobConf property `cascalog.io/tmp-dir-property`."
  [[fs-sym & tmp-syms] & body]
  (let [tmp-root (gensym "tmp-root")]
    `(let [~fs-sym (hadoop/filesystem)
           ~tmp-root (str (get (conf/project-conf) tmp-dir-property "/tmp")
                          "/cascalog_reserved")
           ~@(mapcat (fn [t]
                       [t `(str ~tmp-root "/" (uuid))])
                     tmp-syms)]
       (.mkdirs ~fs-sym (hadoop/path ~tmp-root))
       (try
         ~@body
         (finally
           (delete-all-fs ~fs-sym ~(vec tmp-syms)))))))
