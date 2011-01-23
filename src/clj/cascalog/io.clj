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

(ns cascalog.io
  (:import (java.io File)
           (java.util UUID)
           (org.apache.log4j Logger Level))
  (:use clojure.contrib.java-utils
        [clojure.contrib.duck-streams :only [write-lines]])
  (:use [cascalog util])
  (:require [cascalog [hadoop :as hadoop]]))

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
           (delete-all-fs ~fs-sym ~(vec tmp-syms)))
        ))))
