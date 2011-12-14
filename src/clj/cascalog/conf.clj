(ns cascalog.conf
  (:use [clojure.java.io :only (resource)])
  (:require [cascalog.util :as u]))

(defn read-settings [x]
  (try (binding [*ns* (create-ns (gensym "settings"))]
         (refer 'clojure.core)
         (eval (read-string (str "(do " x ")"))))
       (catch RuntimeException e
         (u/throw-runtime "Error reading job-conf.clj!\n\n" e))))

(defn project-settings []
  (if-let [conf-path (resource "job-conf.clj")]
    (let [conf (-> conf-path slurp read-settings u/project-merge)]
      (u/safe-assert (map? conf)
                     "job-conf.clj must produce a map of config parameters!")
      conf)
    {}))

(def ^:dynamic *JOB-CONF* {})

(defn project-conf []
  (u/project-merge {"cascading.comparator.default" "cascalog.hadoop.DefaultComparator"}
                   (project-settings)
                   *JOB-CONF*
                   {"io.serializations" "cascalog.hadoop.ClojureKryoSerialization"}))
