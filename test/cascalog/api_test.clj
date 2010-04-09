(ns cascalog.api-test
  (:use clojure.test
        cascalog.testing
        cascalog.api)
  (:import [cascading.tuple Fields])
  (:require [cascalog [workflow :as w]]))

; (deftest test-basic-query
;   (with-tmp-sources [friends [["n" "j"] ["n" "a"] ["m" "a"]]
;                      age     [["n" 22] ["m" 30] ["j" 26]] ]
;     (?<- (w/lfs-tap (w/text-line ["p1"]) "/tmp/aaa") [?p1] (age ?p1 ?a) (friends ?p1 _) (< ?a 27))
;    ))

(deftest test-simple-query
  (with-tmp-sources [age [["n" 24] ["n" 23] ["i" 31] ["c" 30] ["j" 21] ["q" nil]]]
     (test?<- [["j"] ["n"]] [?p] (age ?p ?a) (< ?a 25))
     (test?<- [["j"] ["n"] ["n"]] {:distinct false} [?p] (age ?p ?a) (< ?a 25))
    ))

(deftest test-larger-tuples
  (with-tmp-sources [stats [["n" 6 190 nil] ["n" 6 195 nil] ["i" 5 180 31] ["g" 5 150 60]]
                     friends [["n" "i" 6] ["n" "g" 20] ["g" "i" nil]]]
     (test?<- [["g" 60]] [?p ?a] (stats ?p _ _ ?a) (friends ?p _ _))
     (test?<- [] [?p ?a] (stats ?p 1000 _ ?a))
    ))