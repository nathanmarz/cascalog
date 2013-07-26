(ns cascalog.conf
  (:use [cascalog.util :only (project-merge)]
        [clojure.java.io :as io])
  (:require [jackknife.core :as u])
  (:import [cascading.flow FlowProps]
           [cascading.property AppProps]
           [java.util Properties]))

(defn read-settings [x]
  (try (binding [*ns* (create-ns (gensym "settings"))]
         (refer 'clojure.core)
         (eval (read-string (str "(do " x ")"))))
       (catch RuntimeException e
         (u/throw-runtime "Error reading job-conf.clj!\n\n" e))))

(defn project-settings []
  (if-let [conf-path (io/resource "job-conf.clj")]
    (let [conf (-> conf-path slurp read-settings project-merge)]
      (u/safe-assert (map? conf)
                     "job-conf.clj must end with a map of config parameters.")
      conf)
    {}))

(def ^:dynamic *JOB-CONF* {})

(defn project-conf []
  (project-merge {FlowProps/DEFAULT_ELEMENT_COMPARATOR "cascalog.hadoop.DefaultComparator"}
                 (project-settings)
                 *JOB-CONF*
                 {"io.serializations"
                  "cascalog.hadoop.ClojureKryoSerialization",
                  "cascading.compatibility.retain.collector"
                  ; defined in cascading.flow.stream.OperatorStage/RETAIN_COLLECTOR as of 
                  ; Cascading 2.2
                  true}))

(defn set-job-conf! [amap]
  (alter-var-root #'*JOB-CONF* (fn [& ignored] (into {} amap))))

(defn get-version [dep]
  ;; read the project version from the pom.properties file in the jar
  (let [path (str "META-INF/maven/" (name dep) "/pom.properties")
        props (io/resource path)]
    (when props
      (with-open [stream (io/input-stream props)]
        (let [props (doto (Properties.) (.load stream))]
          (.getProperty props "version"))))))

(System/setProperty AppProps/APP_FRAMEWORKS 
  ;; being a good citizen in the cascading ecosystem and set the framework 
  ;; property
       (str, "cascalog:" 
            (get-version "cascalog/cascalog-core")))
