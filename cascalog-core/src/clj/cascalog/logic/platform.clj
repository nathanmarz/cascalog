(ns cascalog.logic.platform
  "The execution platform class."
  (:require [jackknife.core :as u]))

(defprotocol IPlatform
  (generator? [p x]
    "Returns true if the supplied x is a generator, false
    otherwise.")
  (generator [p gen fields options]
    "Returns some source representation."))

(defrecord EmptyPlatform []
  IPlatform
  (generator? [_ _] false)

  (generator [_ _ _ _] nil))

(def ^:dynamic *context* (EmptyPlatform.))

;; Don't use this function, since it's limited in its scope.
;; Instead you should use with-context
(defn set-context! [c]
  (alter-var-root #'*context* (constantly c)))

(defmacro with-context
  [context & body]
  `(binding [*context* ~context]
     ~@body))

(defn gen? [g]
  (generator? *context* g))

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
