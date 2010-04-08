(ns cascalog.core-test
  (:use clojure.test
        cascalog.testing
        cascalog.core)
  (:import [cascading.tuple Fields])
  (:require [cascalog [workflow :as w]]))

(deftest test-basic-query
  (with-tmp-sources [friends {:fields ["p1" "p2"] :tuples [["n" "j"] ["j" "m"] ["m" "a"]]}
                     ages {:fields ["p" "a"] :tuples [["n" 25] ["m" 30]]}]
    (?<- (w/lfs-tap (w/text-line ["p1" "p2"]) "/tmp/aaa") [?p1 ?p3] (friends ?p1 ?p2) (friends ?p2 ?p3))
   ))