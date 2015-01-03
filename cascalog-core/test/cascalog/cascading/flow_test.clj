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
