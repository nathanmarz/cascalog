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