(ns cascalog.fluent.conf
  (:require [jackknife.core :as u]
            [cascalog.util :refer (project-merge)]
            [clojure.java.io :refer (resource)])
  (:import [cascading.flow FlowProps]))

(defn read-settings [x]
  (try (binding [*ns* (create-ns (gensym "settings"))]
         (refer 'clojure.core)
         (eval (read-string (str "(do " x ")"))))
       (catch RuntimeException e
         (u/throw-runtime "Error reading job-conf.clj!\n\n" e))))

(defn project-settings []
  (if-let [conf-path (resource "job-conf.clj")]
    (let [conf (-> conf-path slurp read-settings project-merge)]
      (u/safe-assert (map? conf)
                     "job-conf.clj must end with a map of config parameters.")
      conf)
    {}))

(def ^:dynamic *JOB-CONF* {})

;; TODO: Replace io.serializations with the proper info.
(defn project-conf []
  (project-merge {FlowProps/DEFAULT_ELEMENT_COMPARATOR
                  "cascalog.hadoop.DefaultComparator"}
                 (project-settings)
                 *JOB-CONF*
                 {"io.serializations"
                  "cascalog.hadoop.ClojureKryoSerialization"}))

(defn set-job-conf! [amap]
  (alter-var-root #'*JOB-CONF*
                  (constantly (into {} amap))))

(defmacro with-job-conf
  "Modifies the job conf for queries executed within the form. Nested
   with-job-conf calls will merge configuration maps together, with
   innermost calls taking precedence on conflicting keys."
  [conf & body]
  `(binding [*JOB-CONF*
             (conf-merge *JOB-CONF* ~conf)]
     ~@body))

(defmacro with-serializations
  "Enables the supplied serializations for queries executed within the
  form. Serializations should be provided as a vector of strings or
  classes, like so:

  (import 'org.apache.hadoop.io.serializer.JavaSerialization)
  (with-serializations [JavaSerialization]
     (?<- ...))

  Serializations nest; nested calls to with-serializations will merge
  and unique with serializations currently specified by other calls to
  `with-serializations` or `with-job-conf`."
  [serial-vec & forms]
  `(with-job-conf
     {"io.serializations" (serialization-entry ~serial-vec)}
     ~@forms))
