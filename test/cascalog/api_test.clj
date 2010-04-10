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
     (test?<- [["n"] ["n"]] {:distinct false} [?p] (stats ?p _ _ nil))
    ))

(deftest test-multi-join
  (with-tmp-sources [age [["n" 24] ["a" 15] ["j" 24] ["d" 24] ["b" 15] ["z" 62] ["q" 24]]
                     friends [["n" "a" 16] ["n" "j" 12] ["j" "n" 10] ["j" "d" nil]
                              ["d" "q" nil] ["b" "a" nil] ["j" "a" 1] ["a" "z" 1]]]
        (test?<- [["n" "j"] ["j" "n"] ["j" "d"] ["d" "q"] ["b" "a"]]
          [?p ?p2] (age ?p ?a) (age ?p2 ?a) (friends ?p ?p2 _))
        ))

(deftest test-many-joins
  (with-tmp-sources [age-prizes [[10 "toy"] [20 "animal"] [30 "car"] [40 "house"]]
                     friends    [["n" "j"] ["n" "m"] ["n" "a"] ["j" "a"] ["a" "z"]]
                     age         [["z" 20] ["a" 10] ["n" 15]]]
        (test?<-  [["n" "animal-!!"] ["n" "house-!!"] ["j" "house-!!"]]
          [?p ?prize] (friends ?p ?p2) (friends ?p2 ?p3) (age ?p3 ?a)
                          (* 2 ?a :> ?a2) (age-prizes ?a2 ?prize2) (str ?prize2 "-!!" :> ?prize))
        ))