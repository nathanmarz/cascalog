(ns cascalog.cascading.conf
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [jackknife.core :as u]
            [jackknife.seq :refer (merge-to-vec collectify)])
  (:import [cascading.flow FlowProps]
           [cascading.property AppProps]
           [java.util Properties]))

(def default-serializations
  ["org.apache.hadoop.io.serializer.WritableSerialization"
   "cascading.tuple.hadoop.BytesSerialization"
   "cascading.tuple.hadoop.TupleSerialization"])

(defn serialization-entry
  [serial-vec]
  (->> serial-vec
       (map (fn [x]
              (cond (string? x) x
                    (class? x) (.getName x))))
       (s/join ",")))

(defn no-empties [s]
  (when s (not= "" s)))

(defn merge-serialization-strings
  [& all]
  (serialization-entry
   (->> (filter no-empties all)
        (map #(s/split % #","))
        (apply merge-to-vec default-serializations))))

(defn stringify [x]
  (if (class? x)
    (.getName x)
    (str x)))

;; TODO: Use this to merge together conf maps into the JobConf
;; properly.

(defn stringify-keys [m]
  (into {} (for [[k v] m]
             [(if (keyword? k)
                (name k)
                (str k)) v])))

(defn resolve-collections [v]
  (->> (collectify v)
       (map stringify)
       (s/join ",")))

(defn adjust-vals [& vals]
  (->> (map resolve-collections vals)
       (apply merge-serialization-strings)))

(defn conf-merge [& ms]
  (->> ms
       (map #(u/update-vals % (fn [_ v] (resolve-collections v))))
       (reduce merge)))

(defn project-merge [& ms]
  (let [vals (->> (map #(get % "io.serializations") ms)
                  (apply adjust-vals))
        ms (apply conf-merge ms)]
    (assoc ms "io.serializations" vals)))

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

;; TODO: Replace io.serializations with the proper info.
;; cheating here and hardcoding the ConfiguredInstantiator class
(defn project-conf []
  (project-merge {FlowProps/DEFAULT_ELEMENT_COMPARATOR
                  "cascalog.hadoop.DefaultComparator"
                  "com.twitter.chill.config.configuredinstantiator"
                  "cascalog.kryo.ClojureKryoInstantiator"}
                 (project-settings)
                 *JOB-CONF*
                 {"io.serializations" "com.twitter.chill.hadoop.KryoSerialization"
                  "cascading.compatibility.retain.collector" true}))

(defn set-job-conf! [amap]
  (alter-var-root #'*JOB-CONF*
                  (constantly (into {} amap))))

(defn get-version [dep]
  ;; read the project version from the pom.properties file in the jar
  (let [path (str "META-INF/maven/" (name dep) "/pom.properties")
        props (io/resource path)]
    (when props
      (with-open [stream (io/input-stream props)]
        (let [props (doto (Properties.) (.load stream))]
          (.getProperty props "version"))))))

;; being a good citizen in the cascading ecosystem and set the
;; framework property
(AppProps/addApplicationFramework nil
                    (str "cascalog:" (get-version "cascalog/cascalog-core")))

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
