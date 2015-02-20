(ns cascalog.cascading.stats
  "Namespace implementing stats processing for the Cascading planner."
  (:require [schema.core :as s])
  (:import [cascading.stats CascadingStats]))

;; ## Schemas

(def CounterGroup s/Str)

(def CounterName s/Str)

(s/defschema CounterMap
  {CounterGroup {CounterName s/Int}})

(s/defschema StatsMap
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

(def default-group "CascalogStats")

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

;; ## Stats Output

(defn map-by [f xs]
  (into {} (for [x xs] [x (f x)])))

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

;; ## Canned Stats Functions
