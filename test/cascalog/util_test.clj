(ns cascalog.util-test
  (:use clojure.test)
  (:use cascalog.util))

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

(defmacro throws? [eclass form]
  `(try
    ~form
    (is (= true false))
   (catch ~eclass e#)))

(deftest test-remove-first
  (is (= [1 2 :a] (remove-first keyword? [1 :b 2 :a])))
  (is (= [1 2 3 4 5] (remove-first (partial = 1) [1 1 2 3 4 5])))
  (is (= [1 1 2 3 5] (remove-first (partial = 4) [1 1 2 3 4 5])))
  (throws? IllegalArgumentException (remove-first even? [1 3 5]))
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
  (throws? IllegalArgumentException (unweave ["a" "b" "c"]))
  (throws? IllegalArgumentException (unweave [100]))
  )
