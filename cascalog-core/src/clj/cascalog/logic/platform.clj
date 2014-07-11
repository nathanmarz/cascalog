(ns cascalog.logic.platform
  "The execution platform class."
  (:require [cascalog.logic.zip :as zip]))

;; source-map is a map of identifier to tap, or source. Pipe is the
;; current pipe that the user needs to operate on.

(defrecord ClojureFlow [source-map sink-map trap-map tails pipe name])

;; ## Platform Protocol

(defprotocol IPlatform
  (generator? [p x]
    "Returns true if the supplied x is a generator, false
    otherwise.")
  (generator [p gen fields options]
    "Returns some source representation.")

  (to-generator [p x]))

;; This is required so that the *context* var isn't nil
(defrecord EmptyPlatform []
  IPlatform
  (generator? [_ _] false)

  (generator [_ _ _ _] nil)

  (to-generator [_ _] nil))

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

;; Takes the TailStruct and turns it into a ClojureFlow.
;; TailStruct is a with the last functions on the first level
;; and the Generator (which is a ClojureFlow) on the inner-most level.
;; As the Map is walked by the zip function, it removes the outer
;; level and adds them onto the next level
(defn compile-query [query]
  (zip/postwalk-edit
   (zip/cascalog-zip query)
   identity
   (fn [x _] (to-generator *context* x))
   :encoder (fn [x]
              (or (:identifier x) x))))

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
