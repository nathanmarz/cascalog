(ns cascalog.jcascalog-test
  (:use clojure.test
        cascalog.api
        cascalog.logic.testing
        cascalog.cascading.testing
        cascalog.in-memory.testing)
  (:import [jcascalog Api Option Predicate PredicateMacroTemplate
            PredicateMacro Subquery Api$FirstNArgs]
           [jcascalog.op Count Div Sum Multiply Equals]))

(use-fixtures :once
  (fn [f]
    (Api/setCascadingPlatform)
    (f)
    (Api/setInMemoryPlatform)
    (f)))

(deftest test-vanilla
  (let [value [["a" 1] ["a" 2] ["b" 10]
               ["c" 3] ["b" 2] ["a" 6]]]
    (test?- [["a" 18] ["b" 24] ["c" 6]]
            (-> (Subquery. ["?letter" "?doublesum"])
                (.predicate value ["?letter" "?v"])
                (.predicate (Multiply.) ["?v" 2]) (.out ["?double"])
                (.predicate (Sum.) ["?double"]) (.out ["?doublesum"])))

    (test?- [["a"] ["a"] ["a"]]
            (-> (Subquery. ["?letter"])
                (.predicate value ["?letter" "_"])
                (.predicate (Equals.) ["?letter" "a"])))

    (test?- [["a"]]
            (-> (Subquery. ["?letter"])
                (.predicate value ["?letter" "_"])
                (.predicate #'= ["?letter" "a"])
                (.predicate Option/DISTINCT [true])))))


(def my-avg
    (reify PredicateMacro
      (getPredicates [this [val] [avg]]
        (let [count-var (Api/genNullableVar)
              sum-var (Api/genNullableVar)]
          [(Predicate. (Count.) [count-var])
           (Predicate. (Sum.) [val] [sum-var])
           (Predicate. (Div.) [sum-var count-var] [avg])]))))

(deftest test-java-predicate-macro
  (let [nums [[1] [2] [3] [4] [5]]]
    (test?- [[3]]
            (-> (Subquery. ["?avg"])
                (.predicate nums ["?v"])
                (.predicate my-avg ["?v"]) (.out ["?avg"])))))

(def my-avg-template
  (-> (PredicateMacroTemplate/build ["?v"]) (.out ["?avg"])
      (.predicate (Count.) ["?count"])
      (.predicate (Sum.) ["?v"]) (.out ["?sum"])
      (.predicate (Div.) ["?sum" "?count"]) (.out ["?avg"])))

(deftest test-java-predicate-macro-template
  (let [nums [[1] [2] [3] [4] [5]]]
    (test?- [[3]]
            (-> (Subquery. ["?avg"])
                ;; use ?sum name here to try to confuse it (test that
                ;; it renames intermediate vars)
                (.predicate nums ["?sum"])
                (.predicate my-avg-template ["?sum"]) (.out ["?avg"])))))

(deftest test-first-n
  (let [data [["a" 1] ["a" 1] ["b" 1] ["c" 1] ["c" 1] ["a" 1]
              ["d" 1]]
        sq (-> (Subquery. ["?l" "?count"])
               (.predicate data ["?l" "_"])
               (.predicate (Count.) ["?count"]))
        firstn (Api/firstN sq 2
                           (-> (Api$FirstNArgs.)
                               (.sort "?count")
                               (.reverse true)))]
    (test?- [["c"]]
            (-> (Subquery. ["?l"])
                (.predicate firstn ["?l" 2])))))

(deftest test-java-each
  (let [data [[1 2 3] [4 5 6]]]
    (test?- [[5 7 9]]
            (-> (Subquery. ["?x" "?y" "?z"])
                (.predicate data ["?a" "?b" "?c"])
                (.predicate (Api/each (Sum.))
                            ["?a" "?b" "?c"]) (.out ["?x" "?y" "?z"])))))
