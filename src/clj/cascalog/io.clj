(ns cascalog.io
  (:use cascalog.util
        clojure.java.io)
  (:require [hadoop-util.core :as hadoop])
  (:import [java.io File PrintWriter]
           [java.util UUID]
           [org.apache.log4j Logger Level]))

(defn write-lines
  "Writes lines (a seq) to f, separated by newlines.  f is opened with
  writer, and automatically closed at the end of the sequence."
  [f lines]
  (with-open [^PrintWriter writer (writer f)]
    (loop [lines lines]
      (when-let [line (first lines)]
        (.write writer (str line))
        (.println writer)
        (recur (rest lines))))))

(defn delete-file-recursively
  "Delete file f. If it's a directory, recursively delete all its contents.
Raise an exception if any deletion fails unless silently is true."
  [f & [silently]]
  (let [f (file f)]
    (if (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-file-recursively child silently)))
    (delete-file f silently)))

(defn temp-path [sub-path]
  (file (System/getProperty "java.io.tmpdir") sub-path))

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

(defn delete-all
  "delete-file-recursively is preemptive delete on exiting the code block for
   repl and tests run in the same process."
  [bindings]
  (doseq [file (reverse (map second (partition 2 bindings)))]
    (if (.exists file)
      (delete-file-recursively file))))

(defmacro with-tmp-files [bindings & body]
  `(let ~bindings
     (try ~@body
          (finally (delete-all ~bindings)))))

(defn write-lines-in
  ([root lines]
     (write-lines-in root (str (uuid) ".data") lines))
  ([root filename lines]
     (write-lines
      (file (.getAbsolutePath root) filename) lines)))

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

(defmacro with-fs-tmp [[fs-sym & tmp-syms] & body]
  (let [tmp-paths (mapcat (fn [t]
                            [t `(str "/tmp/cascalog_reserved/" (uuid))])
                          tmp-syms)]
    `(let [~fs-sym (hadoop/filesystem)
           ~@tmp-paths]
       (.mkdirs ~fs-sym (hadoop/path "/tmp/cascalog_reserved"))
       (try
         ~@body
         (finally
          (delete-all-fs ~fs-sym ~(vec tmp-syms)))))))
