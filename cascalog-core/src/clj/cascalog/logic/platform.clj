(ns cascalog.logic.platform
  "The execution platform class."
  (:require [cascalog.logic.zip :as zip]))

;; ## Platform Protocol

(defprotocol IPlatform
  (generator? [p x]
    "Returns true if the supplied x is a generator, false
    otherwise.")
  (generator [p gen fields options]
    "Returns some source representation.")

  (to-generator [p x])

  (run! [p compiled-queries])

  (run-memory! [p name compiled-queries]))

;; This is required so that the *context* var isn't nil
(defrecord EmptyPlatform []
  IPlatform
  (generator? [_ _] false)

  (generator [_ _ _ _] nil)

  (to-generator [_ _] nil)

  (run! [_ _ ] nil)

  (run-memory! [_ _ _] nil))

(def ^:dynamic *context* (EmptyPlatform.))

(defn set-context! [c]
  (alter-var-root #'*context* (constantly c)))

(defmacro with-context
  [context & body]
  `(binding [*context* ~context]
     ~@body))

(defn gen? [g]
  (generator? *context* g))

(defn compile-query [query]
  (zip/postwalk-edit
   (zip/cascalog-zip query)
   identity
   (fn [x _] (to-generator *context* x))
   :encoder (fn [x]
              (or (:identifier x) x))))

(defn run-query! [compiled-queries]
  (run! *context* compiled-queries))

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
