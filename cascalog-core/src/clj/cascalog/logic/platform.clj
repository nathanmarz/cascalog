(ns cascalog.logic.platform
  "The execution platform class."
  (:require [cascalog.logic.zip :as zip]
            [jackknife.core :as u]))

;; ## Platform Protocol
(defprotocol IPlatform
  (generator-platform? [p x]
    "Returns true if the supplied x is a generator, false
    otherwise.")
  
  (generator-platform [p gen fields options]
    "Returns some source representation.")
  
  (run! [p name bindings])

  (run-to-memory! [p name compiled-queries]))

;; This is required so that the *context* var isn't nil
(defrecord EmptyPlatform []
  IPlatform
  (generator-platform? [p _]
    (u/throw-illegal (str p " isn't a valid platform.")))

  (generator-platform [p _ _ _]
    (u/throw-illegal (str p " isn't a valid platform.")))

  (run! [p _ _]
    (u/throw-illegal (str p " isn't a valid platform.")))

  (run-to-memory! [p _ _]
    (u/throw-illegal (str p " isn't a valid platform."))))

(def ^:dynamic *context* (EmptyPlatform.))

(defn set-context! [c]
  (alter-var-root #'*context* (constantly c)))

(defmacro with-context
  [context & body]
  `(binding [*context* ~context]
     ~@body))

(defn gen-dispatch
  [gen]
  [(type *context*) (type gen)])

(defmulti generator gen-dispatch)

(defmulti to-generator
  (fn [item]
    [(type *context*) (type item)]))

(defn generator?
  "Evaluates whether there is a method to dispatch to for the
  for the multimethod."
  [g]
  (not (nil?
        (.getMethod generator (gen-dispatch g)))))

(defn compile-query [query]
  (zip/postwalk-edit
   (zip/cascalog-zip query)
   identity
   (fn [x _] (to-generator x))
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
