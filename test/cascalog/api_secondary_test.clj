(ns cascalog.api-secondary-test
  (:use clojure.test
        cascalog.testing
        cascalog.api)
  (:import [cascading.tuple Fields])
  (:require [cascalog [ops :as c]]))

(deftest test-outfields-query
  (with-tmp-sources [age [["nathan" 25]]]
    (is (= ["?age"] (get-out-fields (<- [?age] (age _ ?age)))))
    (is (= ["!!age2" "!!age"] (get-out-fields (<- [!!age2 !!age] (age ?person !!age) (age ?person !!age2)))))
    (is (= ["?person" "!a"] (get-out-fields (<- [?person !a] (age ?person !a)))))
    (is (= ["!a" "!count"] (get-out-fields (<- [!a !count] (age _ !a) (c/count !count)))))
    ))

(deftest test-outfields-tap
  (is (thrown? IllegalArgumentException (get-out-fields (memory-source-tap Fields/ALL []))))
  (is (= ["!age"] (get-out-fields (memory-source-tap ["!age"] []))))
  (is (= ["?age" "field2"] (get-out-fields (memory-source-tap ["?age" "field2"] []))))
  )

(defbufferop sum+1 [tuples]
  [(inc (reduce + (map first tuples)))])

(defn op-to-pairs [sq op]
  (<- [?c] (sq ?a ?b) (op ?a ?b :> ?c) (:distinct false)))

(deftest test-use-var
  (with-tmp-sources [nums [[1 1] [2 2] [1 3]]]
    (test?<- [[2] [4] [4]] [?sum] (nums ?a ?b) (#'+ ?a ?b :> ?sum) (:distinct false))
    (test?- [[0] [0] [-2]] (op-to-pairs nums #'-))
    (test?- [[5]] (op-to-pairs nums #'sum+1))
    (test?- [[5]] (op-to-pairs nums sum+1))
    ))
