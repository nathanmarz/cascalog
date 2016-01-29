(ns cascalog.logic.platform
  "The execution platform class."
  (:refer-clojure :exclude [run!])
  (:require [cascalog.logic.zip :as zip]
            [jackknife.core :as u]))

;; ## Platform Protocol
(defprotocol IPlatform
  (generator? [p x]
    "Returns true if the supplied x is a generator, false
    otherwise.")

  (generator-builder [p gen fields options]
    "Returns some source representation.")

  (run! [p name bindings])

  (run-to-memory! [p name queries]))

;; This is required so that the *platform* var isn't nil
(defrecord EmptyPlatform []
  IPlatform
  (generator? [p _]
    (u/throw-illegal (str p " isn't a valid platform.")))

  (generator-builder [p _ _ _]
    (u/throw-illegal (str p " isn't a valid platform.")))

  (run! [p _ _]
    (u/throw-illegal (str p " isn't a valid platform.")))

  (run-to-memory! [p _ _]
    (u/throw-illegal (str p " isn't a valid platform."))))

(def ^:dynamic *platform* (EmptyPlatform.))

(defn set-platform! [c]
  (alter-var-root #'*platform* (constantly c)))

(defmacro with-platform
  [platform & body]
  `(binding [*platform* ~platform]
     ~@body))

(defn gen-dispatch
  "Dispatch for the generator multimethod."
  [gen]
  [(type *platform*) (type gen)])

(defmulti generator
  "Accepts some type and returns a platform specific representation
   that can be used as a generator."
  gen-dispatch)

(defn platform-generator?
  "Evaluates whether there is a method to dispatch to for the
  generator multimethod."
  [g]
  (not (nil?
        (.getMethod generator (gen-dispatch g)))))

(defmulti to-generator
  (fn [item]
    [(type *platform*) (type item)]))

(defn compile-query [query]
  (zip/postwalk-edit
   (zip/cascalog-zip query)
   identity
   (fn [x _] (to-generator x))
   :encoder (fn [x]
              (or (:identifier x) x))))
