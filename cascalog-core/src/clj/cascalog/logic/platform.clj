(ns cascalog.logic.platform
  "The execution platform class."
  (:require [jackknife.core :as u]))

;; ## Generator Protocol

(defprotocol IGenerator
  "Accepts some type and returns a ClojureFlow that can be used as a
  generator. The idea is that a clojure flow can always be used
  directly as a generator."
  (generator [x]))

;; ## Platform Protocol

(defprotocol IPlatform
  (pgenerator? [p x]
    "Returns true if the supplied x is a generator, false
    otherwise.")
  (pgenerator [p gen fields options]
    "Returns some source representation."))

;; This is required so that the *context* var isn't nil
(defrecord EmptyPlatform []
  IPlatform
  (pgenerator? [_ _] false)

  (pgenerator [_ _ _ _] nil))

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
  (pgenerator? *context* g))

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
