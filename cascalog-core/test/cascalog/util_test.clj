(ns cascalog.util-test
  (:refer-clojure :exclude [flatten memoize])
  (:use cascalog.util
        midje.sweet))

(tabular
 (fact "test-all-pairs"
   (all-pairs ?input) => ?result)
 ?input      ?result
 [1]         []
 [1 2 3]     [[1 2] [1 3] [2 3]]
 [1 :a :a 2] [[1 :a] [1 :a] [1 2] [:a :a] [:a 2] [:a 2]])

(facts "count= tests."
  (count= [1] []) => false

  (count= [1] [1] [3]) => true
  (count= [1 2] [4 3]) => true

  (not-count= [1] []) => true
  (not-count= [1 2] [3 4] []) => true
  (not-count= [1] [1]) => false
  (not-count= [1 2] [4 3]) => false)
