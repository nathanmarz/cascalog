(ns cascalog.clojure.platform
  (:require [cascalog.logic.predicate]
            [cascalog.logic.platform :refer
             (compile-query generator IGenerator to-generator IRunner IPlatform)]
            [cascalog.logic.parse :as parse]
            [jackknife.core :as u])
  (:import [cascalog.logic.platform ClojureFlow]
           [cascalog.logic.parse TailStruct Projection Application]
           [cascalog.logic.predicate Generator RawSubquery]))

(defrecord ClojurePlatform []
  IPlatform
  (pgenerator? [_ x]
    (satisfies? IGenerator x))

  (pgenerator [_ gen output options]
    (let [id (u/uuid)]
      (ClojureFlow. {id gen} nil nil nil nil nil)))

  (pto-generator [_ x]
    (to-generator x)))

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

(extend-protocol IRunner
  Projection
  (to-generator [{:keys [source fields]}]
    (prn "in herer" source)
    (select-fields fields source))

  Generator
  (to-generator [{:keys [gen fields]}]
    (let [coll (first ( vals (:source-map gen)))
          res (name-tuples fields coll)]
      (prn "in herrre" gen " and " fields)
      res))

  TailStruct
  (to-generator [item]
    (prn "i'm over her")
    (:node item)))

(extend-protocol IGenerator
  TailStruct
  (generator [sq]
    (compile-query sq))

  RawSubquery
  (generator [sq]
    (generator (parse/build-rule sq))))

