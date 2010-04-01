(ns cascalog.util-test
  (:use clojure.test)
  (:use cascalog.util))

; (defn transpose [m]
; (defn substitute-if [pred subfn aseq]
; (defn try-resolve [obj]
; (defn collectify [obj]
; (defn multi-set [aseq]

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

