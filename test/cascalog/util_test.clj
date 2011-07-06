(ns cascalog.util-test
  (:use clojure.test)
  (:use cascalog.util))

(deftest test-transpose
  (is (= [[1 2] [3 4]] (transpose [[1 3] [2 4]]))))

(deftest test-wipe
  (are [idx coll res] (= res (wipe coll idx))
       1 [1 2 3 4] [1 3 4]
       0 [1 2 3 4] [2 3 4]
       6 [1 2 3 4] [1 2 3 4]))

(def p 5)

(deftest test-try-resolve
  (binding [*ns* (find-ns 'cascalog.util-test)]
    (is (= nil (try-resolve 'qqq)))
    (is (= #'+ (try-resolve '+)))
    (let [a 1]
      (is (= nil (try-resolve 'a))))
    (is (= nil (try-resolve 10)))
    (is (= nil (try-resolve [1 2 3])))
    (is (= #'p (try-resolve 'p)))))

(deftest test-collectify
  (is (= '(1 2 3) (collectify '(1 2 3))))
  (is (= [5 5] (collectify [5 5])))
  (is (= ["aaa"] (collectify "aaa")))
  (is (= [{:a 1 :b 2}] (collectify {:a 1 :b 2}))))

(deftest test-remove-first
  (is (= [1 2 :a] (remove-first keyword? [1 :b 2 :a])))
  (is (= [1 2 3 4 5] (remove-first (partial = 1) [1 1 2 3 4 5])))
  (is (= [1 1 2 3 5] (remove-first (partial = 4) [1 1 2 3 4 5])))
  (is (thrown? IllegalArgumentException (remove-first even? [1 3 5])))
  )

(deftest test-all-pairs
  (is (= [] (all-pairs [1])))
  (is (= [[1 2] [1 3] [2 3]] (all-pairs [1 2 3])))
  (is (= [[1 :a] [1 :a] [1 2] [:a :a] [:a 2] [:a 2]] (all-pairs [1 :a :a 2])))
  )

(deftest test-unweave
  (is (= [[1 3 5] [2 4 6]] (unweave [1 2 3 4 5 6])))
  (is (= [["a" "q"] [99 "c"]] (unweave ["a" 99 "q" "c"])))
  (is (= [[] []] (unweave [])))
  (is (thrown? IllegalArgumentException (unweave ["a" "b" "c"])))
  (is (thrown? IllegalArgumentException (unweave [100])))
  )

(deftest test-duplicates
  (is (= [1 2]) (duplicates [1 2 2 1 3]))
  (is (= []) (duplicates (range 4)))
  (is (= ["face"]) (duplicates [1 "face" 2 "face"])))

(deftest test-count=
  (is (= false (count= [1] [])))
  (is (= true (count= [1] [1] [3])))
  (is (= true (count= [1 2] [4 3])))

  (is (= true (not-count= [1] [])))
  (is (= true (not-count= [1 2] [3 4] [])))
  (is (= false (not-count= [1] [1])))
  (is (= false (not-count= [1 2] [4 3])))
  )
