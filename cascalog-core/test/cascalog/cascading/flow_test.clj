(ns cascalog.cascading.flow-test
  (:use [midje sweet cascalog]
        clojure.test
        cascalog.logic.testing
        cascalog.cascading.testing
        cascalog.api)
  (:require [cascalog.cascading.operations :as ops]
            [cascalog.logic.platform :as p]
            [cascalog.cascading.flow :as f]))

(background
 (before :facts
         (set-cascading-platform!)))

(defn square [x]
  (* x x))

(deftest to-memory-test
  (let [gen (-> (p/generator [1 2 3 4])
                (ops/rename* "?x")
                (ops/map* square "?x" "?x2"))]
    (fact
     (f/to-memory gen)
     => [[1 1] [2 4] [3 9] [4 16]])))

(comment
  "Turn these into valid tests."
  (require '[cascalog.logic.parse :refer (<-)]
           '[cascalog.cascading.flow :refer (all-to-memory to-memory graph)])

  (def cross-join
    (<- [:>] (identity 1 :> _)))

  (let [sq (<- [?squared ?squared-minus ?x ?sum]
               ([1 2 3] ?x)
               (* ?x ?x :> ?squared)
               (- ?squared 1 :> ?squared-minus)
               ((d/parallelagg* +) ?squared :> ?sum))]
    (to-memory sq))

  (let [sq (<- [?x ?y]
               ([1 2 3] ?x)
               ([1 2 3] ?y)
               (cross-join)
               (* ?x ?y :> ?z))]
    (to-memory sq))

  (let [x (<- [?x ?y :> ?z]
              (* ?x ?x :> 10)
              (* ?x ?y :> ?z))
        sq (<- [?a ?b ?z]
               ([[1 2 3]] ?a)
               (x ?a ?a :> 4)
               ((d/bufferop* +) ?a :> ?z)
               ((d/mapcatop* +) ?a 10 :> ?b))]
    (clojure.pprint/pprint (build-rule sq))))
