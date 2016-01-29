(ns cascalog.cascading.stats
  "Namespace implementing stats processing for the Cascading planner."
  (:require [clojure.string :refer [join]]
            [schema.core :as s])
  (:import [cascading.stats CascadingStats]))

;; ## Schemas

(def CounterGroup s/Str)

(def CounterName s/Str)

(s/defschema CounterMap
  "Clojure representation of Cascading's counters."
  {CounterGroup {CounterName s/Int}})

(s/defschema StatsMap
  "Clojure representation of the cascading.stats.CascadingStats data
  structure."
  {:name (s/maybe s/Str)
   :counters CounterMap
   :duration s/Int
   :finished-time s/Int
   :id s/Str
   :run-time s/Int
   :start-time s/Int
   :submit-time s/Int
   :failed? s/Bool
   :skipped? s/Bool
   :stopped? s/Bool
   :successful? s/Bool})

;; ## Dynamic Variables
;;
;; These variables are bound within the context of a Cascalog job. You
;; can access them from your operations without worry about prepfn
;; craziness.

(def ^:dynamic *flow-process* nil)
(def ^:dynamic *op-call* nil)

(def default-group
  "This is the default group name for any stats recorded in the course
  of a Cascalog job."
  "CascalogStats")

;; ## Stats Entry

(s/defn inc-by!
  "Increments the supplied counter in the supplied group by, you
  guessed it, the supplied amount. Only takes effect in the context of
  a Cascading flow."
  ([counter :- CounterName value :- s/Int]
     (inc-by! default-group counter value))
  ([group :- CounterGroup counter :- CounterName value :- s/Int]
     (when-let [fp *flow-process*]
       (.increment fp group counter value))))

(s/defn inc!
  "Increments the supplied counter in the supplied group by 1. Only
  takes effect in the context of a Cascading flow."
  ([counter :- CounterName]
     (inc-by! default-group counter 1))
  ([group :- CounterGroup counter :- CounterName]
     (inc-by! group counter 1)))

;; ## Stats Utilities

(defn map->json
  "My own little JSON fn to prevent pulling in a JSON library."
  [m]
  (->> (for [[k v] m :let [v (cond (nil? v) "null"
                                   (string? v) (format " \"%s\"" v)
                                   (map? v) (map->json v)
                                   :else v)]]
         (format "\"%s\": %s" k v))
       (join ", " )
       (format "{%s}")))

(defn map-by
  "Takes a value-generating function and a sequence and returns a map
  with the original seq elements as keys, and (f key) as each
  value."
  [f xs]
  (into {} (for [x xs] [x (f x)])))

;; ## Cascading Stats Processing
;;
;; These functions turn the basic Cascading objects into Clojure data
;; structures.

(s/defn counter-map :- CounterMap
  "Digests the supplied stats object and returns a map of "
  [stats :- CascadingStats]
  (letfn [(counter-v [group counter]
            (.getCounterValue stats group counter))
          (build-group [g]
            (let [counters (.getCountersFor stats g)]
              (map-by (partial counter-v g) counters)))]
    (map-by build-group (.getCounterGroups stats))))

(s/defn stats-map :- StatsMap
  "Returns a Clojure map of relevant stats from the Cascading stats
  object."
  [stats :- CascadingStats]
  {:counters (counter-map stats)
   :duration (.getDuration stats)
   :finished-time (.getFinishedTime stats)
   :id (.getID stats)
   :name (.getName stats)
   :run-time (.getRunTime stats)
   :start-time (.getStartTime stats)
   :submit-time (.getSubmitTime stats)
   :failed? (.isFailed stats)
   :skipped? (.isSkipped stats)
   :stopped? (.isStopped stats)
   :successful? (.isSuccessful stats)})

;; ## :stats-fn Implementations
;;
;; Drop these in to your Cascalog job as `(:stats-fn <f>)`, or wrap
;; your job call in `(with-stats <f> ....)` to use them with the bare
;; Cascading DSL.

(s/defn stdout
  ([] (stdout default-group))
  ([group :- CounterGroup]
     (s/fn [stats :- StatsMap]
       (println (format "Custom counters for group %s:" group))
       (doseq [[counter value] (get (:counters stats) group)]
         (println (format "%s\t%s" counter value))))))

(s/defn to-file
  "path is passed to clojure.java.io/writer, so the argument can be a
   Writer, BufferedWriter, OutputStream, File, URI, URL, Socket, and
   String."
  [path json? :- s/Bool]
  (s/fn [stats :- StatsMap]
    (spit path (if json? (map->json stats) (pr-str stats)))))

(def clojure-file
  "Returns a stats handler that prints the final stats map to the
  supplied file (or output stream, etc, see clojure.java.io/writer) as
  a Clojure data structure."
  #(to-file % false))

(def json-file
  "Returns a stats handler that prints the final stats map to the
  supplied file (or output stream, etc, see clojure.java.io/writer) as
  a JSON data structure."
  #(to-file % true))
