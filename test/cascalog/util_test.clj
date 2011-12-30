(ns cascalog.util-test
  (:use clojure.test
        cascalog.util
        [jackknife.core :only (update-vals)]
        [jackknife.seq :only (unweave)]))

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

(deftest test-all-pairs
  (is (= [] (all-pairs [1])))
  (is (= [[1 2] [1 3] [2 3]] (all-pairs [1 2 3])))
  (is (= [[1 :a] [1 :a] [1 2] [:a :a] [:a 2] [:a 2]] (all-pairs [1 :a :a 2]))))

(deftest test-count=
  (is (= false (count= [1] [])))
  (is (= true (count= [1] [1] [3])))
  (is (= true (count= [1 2] [4 3])))

  (is (= true (not-count= [1] [])))
  (is (= true (not-count= [1 2] [3 4] [])))
  (is (= false (not-count= [1] [1])))
  (is (= false (not-count= [1 2] [4 3]))))

(deftest conf-merge-test
  (let [m1 {"key" "foo"
            "key2" ["bar" "baz"]}
        m2 {"key" ["cake" "salad"]}]
    (is (= {"key" "foo", "key2" "bar,baz"}) (conf-merge m1))
    (is (= {"key" "cake,salad", "key2" "bar,baz"}) (conf-merge m1 m2))))

(deftest stringify-test
  ;; TODO: Test for duplicate keys btw str and kwd
  (is (= {"key" "val" "key2" "val2"}
         (stringify-keys {:key "val" "key2" "val2"}))))
