(ns cascalog.clojure.platform
  (:require [cascalog.logic.predicate]
            [cascalog.logic.platform :refer (compile-query  IPlatform)]
            [cascalog.logic.parse :as parse]
            [jackknife.core :as u])
  (:import [cascalog.logic.platform ClojureFlow]
           [cascalog.logic.parse TailStruct Projection Application]
           [cascalog.logic.predicate Generator RawSubquery]))


;; Generator
(defn name-tuples
  [names tuples]
  (map
   (fn [tup]
       (zipmap names tup))
   tuples))

;; Projection
(defn select-fields
  [fields tuples]  
  (remove nil?
          (map
           (fn [tup]
               (map
                (fn [k]
                    (tup k))
                fields)
               )
           tuples)))		

(defprotocol IRunner
  (to-generator [item]))

(extend-protocol IRunner
  Projection
  (to-generator [{:keys [source fields]}]
    (select-fields fields source))

  Generator
  (to-generator [{:keys [gen fields]}]
    (let [coll (first ( vals (:source-map gen)))
          res (name-tuples fields coll)]
      res))

  TailStruct
  (to-generator [item]
    (:node item)))

(defprotocol IGenerator
  (generator [x]))

(extend-protocol IGenerator
  TailStruct
  (generator [sq]
    (compile-query sq))

  RawSubquery
  (generator [sq]
    (generator (parse/build-rule sq)))

  ClojureFlow
  (generator [x] x))


(defrecord ClojurePlatform []
  IPlatform
  (generator? [_ x]
    ;; TODO: expand this to handle other types (like cascading.platform's)
    true)

  (generator [_ gen output options]
    (let [id (u/uuid)]
      (ClojureFlow. {id gen} nil nil nil nil nil)))

  (to-generator [_ x]
    (to-generator x)))
