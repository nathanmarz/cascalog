(ns cascalog.api-test
  (:use clojure.test
        cascalog.testing
        cascalog.api)
  (:import [cascading.tuple Fields])
  (:require [cascalog [workflow :as w]]))

(deftest test-basic-query
  (with-tmp-sources [friends {:fields ["p1" "p2"] :tuples [["n" "j"] ["j" "m"] ["m" "a"]]}
                     age {:fields ["p" "a"] :tuples [["n" nil] ["m" 30] ["j" 26]]}]
    (?<- (w/lfs-tap (w/text-line ["p1" "p2"]) "/tmp/aaa") [?p1 ?p3] (age ?p1 ?a) (friends ?p1 ?p2) (friends ?p2 ?p3))
   ))