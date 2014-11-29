(ns cascalog.logic.platform
  "The execution platform class."
  (:require [cascalog.logic.zip :as zip]))

;; ## Platform Protocol

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
  
  (run! [p name bindings])

  (run-to-memory! [p name compiled-queries]))

;; This is required so that the *context* var isn't nil
(defrecord EmptyPlatform []
  IPlatform
  (generator-platform? [_ _] false)

  (generator-platform [_ _ _ _] nil)

  (run! [_ _ _] nil)

  (run-to-memory! [_ _ _] nil))

(def ^:dynamic *context* (EmptyPlatform.))

(defn set-context! [c]
  (alter-var-root #'*context* (constantly c)))

(defmacro with-context
  [context & body]
  `(binding [*context* ~context]
     ~@body))

(comment
  (defn generator? [g]
    (generator-platform? *context* g)))

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
