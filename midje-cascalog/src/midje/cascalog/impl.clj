(ns midje.cascalog.impl
  (:use midje.sweet
        [clojure.set :only (difference)]
        [cascalog.api :only (with-job-conf <- ??-)])
  (:require cascalog.cascading.types
            [cascalog.cascading.io :as io]
            [cascalog.cascading.flow :as flow]
            [midje.checking.core :as checking])
  (:import [cascalog.cascading.types ClojureFlow]))

(defn- multifn? [x]
  (instance? clojure.lang.MultiFn x))

(def ^{:private true} mocking-forms
  #{'against-background 'provided})

(defn- mocking-form?
  "Returns true if the supplied form (or sequence) is a midje
   `provided` or `against-background` clause, false otherwise."
  [x]
  (when (coll? x)
    (contains? mocking-forms (first x))))

(defn- extract-mockers
  "Returns a vector of two sequences, obtained by splitting the
  supplied `coll` into midje forms and rest."
  [coll]
  ((juxt filter remove) mocking-form? coll))

(def ^{:private true} default-log-level :fatal)

(defn pop-log-level
  "Accepts a sequence with an optional log level as its first argument
  and returns a 2-vector with the log level (or nil if it wasn't
  present) and the non-log-level elements of the sequence."
  [bindings]
  (let [[pre [ll & more]] (split-with (complement io/log-levels) bindings)]
    (if ll
      [ll (concat pre more)]
      [default-log-level bindings])))

(defn execute
  "Executes the supplied query and returns the sequence of tuples it
  generates. Optionally accepts a log-level key."
  [query & {:keys [log-level] :or {log-level default-log-level}}]
  (io/with-log-level log-level
    (with-job-conf {"io.sort.mb" 10}
      (if (instance? ClojureFlow query)
        (flow/to-memory query)
        (first (??- query))))))

;; ## Midje-Style Checker Helpers

(def log-level-set
  (set (keys io/log-levels)))

(defn mk-opt-set
  "Accepts a sequence of options and returns the same sequence with
  all log-level keywords removed."
  [opts]
  (difference (set opts) log-level-set))

(defn valid-options?
  "Returns false if supplied-opts contains any item not present in
  `permitted-opts` or `log-level-set`, true otherwise."
  [permitted-opts supplied-opts]
  (empty? (difference (set supplied-opts)
                      log-level-set
                      (set permitted-opts))))

(def ^{:doc "Accepts a sequence of arguments to a
  collection-checker-generator and returns a vector containing two
  sequences:

  [<fn arguments> <keyword arguments>]

  fn-arguments are non-keywords meant to pass through unmolested into
  the checker. keyword arguments are optionally parsed by the wrapping
  checker."}
  split-forms
  (partial split-with (complement keyword?)))
