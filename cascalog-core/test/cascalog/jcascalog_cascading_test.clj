(ns cascalog.jcascalog-cascading-test
  (:use clojure.test
        cascalog.api
        cascalog.logic.testing
        cascalog.cascading.testing)
  (:import [cascalog.test MultiplyAgg RangeOp DoubleOp]
           [jcascalog Api Subquery]
           [jcascalog.op Count]))

(use-fixtures :once
  (fn [f]
    (Api/setCascadingContext)
    (f)))

(deftest test-vanilla
  (let [value [["a" 1] ["a" 2] ["b" 10]
               ["c" 3] ["b" 2] ["a" 6]]]
    (test?- [[(* 1 2 3628800 6 2 720) 24]]
            (-> (Subquery. ["?result" "?count"])
                (.predicate value ["_" "?v"])
                (.predicate (RangeOp.) ["?v"]) (.out ["?v2"])
                (.predicate (MultiplyAgg.) ["?v2"]) (.out ["?result"])
                (.predicate (Count.) ["?count"])))))

(deftest test-java-each
  (let [data [[1 2 3] [4 5 6]]]
    (test?- [[2 4 6] [8 10 12]]
            (-> (Subquery. ["?x" "?y" "?z"])
                (.predicate data ["?a" "?b" "?c"])
                (.predicate (Api/each (DoubleOp.))
                            ["?a" "?b" "?c"]) (.out ["?x" "?y" "?z"])))))
