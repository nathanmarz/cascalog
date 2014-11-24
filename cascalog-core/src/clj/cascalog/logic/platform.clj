(ns cascalog.logic.platform
  "The execution platform class."
  (:require [cascalog.logic.zip :as zip]))

;; ## Platform Protocol

(defprotocol IGenerator
  (generator [x]))

(defprotocol IRunner
  (to-generator [item]))

(defprotocol ISink
  (to-sink [this]
    "Returns a Cascading tap into which Cascalog can sink the supplied
    data."))

(defprotocol IPlatform
  (generator-platform? [p x]
    "Returns true if the supplied x is a generator, false
    otherwise.")
  
  (generator-platform [p gen fields options]
    "Returns some source representation.")

  (to-generator-platform [p x])

  (run! [p name bindings])

  (run-memory! [p name compiled-queries]))

;; This is required so that the *context* var isn't nil
(defrecord EmptyPlatform []
  IPlatform
  (generator-platform? [_ _] false)

  (generator-platform [_ _ _ _] nil)

  (to-generator-platform [_ _] nil)

  (run! [_ _ _] nil)

  (run-memory! [_ _ _] nil))

(def ^:dynamic *context* (EmptyPlatform.))

(defn set-context! [c]
  (alter-var-root #'*context* (constantly c)))

(defmacro with-context
  [context & body]
  `(binding [*context* ~context]
     ~@body))

(defn generator? [g]
  (generator-platform? *context* g))

(defn compile-query [query]
  (zip/postwalk-edit
   (zip/cascalog-zip query)
   identity
   (fn [x _] (to-generator-platform *context* x))
   :encoder (fn [x]
              (or (:identifier x) x))))

(defn run-query! [name bindings]
  (run! *context* name bindings))

(defn run-query-memory! [name compiled-queries]
  (run-memory! *context* name compiled-queries))

;; TODO: this is cascading specific and should be moved
(comment
  (require '[cascalog.cascading.flow :as f])
  "TODO: Convert to test."
  (let [gen (-> (types/generator [1 2 3 4])
                (ops/rename* "?x"))
        pred (to-predicate * ["?a" "?a"] ["?b"])]
    (fact
     (f/to-memory
      ((:op pred) gen ["?x" "?x"] "?z"))
     => [[1 1] [2 4] [3 9] [4 16]])))
