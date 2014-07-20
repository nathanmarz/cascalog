(ns cascalog.cascading-api-test
  (:use clojure.test
        [midje sweet cascalog]
        cascalog.logic.testing
        cascalog.api)
  (:require [cascalog.logic.ops :as c]
            [cascalog.cascading.io :as io]
            [cascalog.logic.def :as d])
  (:import [cascading.tuple Fields]
           [cascalog.test KeepEven OneBuffer]
           [cascalog.ops IdentityBuffer]
           [cascading.operation.text DateParser]))

;; Set the context to Cascading
(use-fixtures :once
              (fn  [f]
                (set-cascading-context!)
                (f)))

(deftest test-cascalog-tap-source
  (let [num [[1]]
        gen (<- [?n]
                (num ?raw)
                (inc ?raw :> ?n))
        tap1 (cascalog-tap num nil)]
    (test?<- [[1]] [?n] (tap1 ?n))
    (test?<- [[2]] [?n]
             ((cascalog-tap gen nil) ?n))
    (test?<- [[1]] [?n]
             ((cascalog-tap (cascalog-tap tap1 nil) nil) ?n))))


(deftest test-cascalog-tap-sink
  (let [num [[2]]]
    (with-expected-sinks [sink1 [[2]]
                          sink2 [[3]]
                          sink3 [[2]]]
      (?<- (cascalog-tap nil sink1)
           [?n]
           (num ?n))

      (?<- (cascalog-tap nil (fn [sq] [sink2 (<- [?n2]
                                                 (sq ?n)
                                                 (inc ?n :> ?n2)
                                                 (:distinct false))]))
           [?n]
           (num ?n))

      (?<- (cascalog-tap nil (cascalog-tap nil sink3))
           [?n]
           (num ?n)))))

(deftest test-cascalog-tap-source-and-sink
  (with-expected-sinks [sink1 [[4]]]
    (let [tap (cascalog-tap [[3]] sink1)]
      (?<- tap [?n]
           (tap ?raw)
           (inc ?raw :> ?n)))))

(deftest test-flow-name
  (let [nums [[1] [2]]]
    (with-expected-sinks [sink1 [[1] [2]]
                          sink2 [[2] [3]]]
      (is (= "lalala"
             (:name (compile-flow "lalala" (stdout) (<- [?n] (nums ?n))))))
      (?<- "flow1" sink1
           [?n]
           (nums ?n)
           (:distinct false))
      (?- "flow2" sink2
          (<- [?n2]
              (nums ?n)
              (inc ?n :> ?n2)
              (:distinct false)))
      (is (= '(([1] [2]))
             (??- "flow3"
                  (<- [?n]
                      (nums ?n)
                      (:distinct false))))))))

(deftest test-cascading-function
  (test?<- [["2013-01-01" 1356998400000]]
           [!date !date-millis]
           ([["2013-01-01"]] !date)
           ((DateParser. "yyyy-MM-dd") !date :> !date-millis)))

(deftest test-cascading-filter
  (let [vals [[0] [1] [2] [3]]]
    (test?<- [[0] [2]]
             [?v]
             (vals ?v)
             ((KeepEven.) ?v))

    (test?<- [[0 true] [1 false]
              [2 true] [3 false]]
             [?v ?b]
             (vals ?v)
             ((KeepEven.) ?v :> ?b))))


(deffilterfn odd-fail [n & all]
  (or (even? n)
      (throw (RuntimeException.))))

(deftest test-java-buffer
  (let [vals [["a" 1 10] ["a" 2 20] ["b" 3 31]]]
    (test?<- [["a" 1] ["b" 1]]
             [?f1 ?o]
             (vals ?f1 _ _)
             ((OneBuffer.) :> ?o))

    (test?<- [["a" 1 10] ["a" 2 20] ["b" 3 31]]
             [?f1 ?f2out ?f3out]
             (vals ?f1 ?f2 ?f3)
             ((IdentityBuffer.) ?f2 ?f3 :> ?f2out ?f3out))))
(deftest test-trap-isolation
  (let [num [[1] [2]]]
    (is (thrown? Exception
                 (with-expected-sink-sets [trap1 [[]] ]
                   (let [sq (<- [?n] (num ?n) (odd-fail ?n))]
                     (test?<- [[2]]
                              [?n]
                              (sq ?n)
                              (:trap trap1))))))
    (with-expected-sink-sets [trap1 [[1]]]
      (let [sq (<- [?n]
                   (num ?n)
                   (odd-fail ?n)
                   (:trap trap1))]
        (test?<- [[2]]
                 [?n]
                 (sq ?n))))))

;; Testing Cascading Taps and Traps

(deftest test-select-fields-tap
  (let [data (memory-source-tap ["f1" "f2" "f3" "f4"]
                                [[1 2 3 4] [11 12 13 14] [21 22 23 24]])]
    (test?<- [[4 2] [14 12] [24 22]]
             [?a ?b]
             ((select-fields data ["f4" "f2"]) ?a ?b))

    (test?<- [[1 3 4] [11 13 14] [21 23 24]]
             [?f1 ?f2 ?f3]
             ((select-fields data ["f1" "f3" "f4"]) ?f1 ?f2 ?f3))))

(deftest memory-self-join-test
  (let [src  [["a"]]
        src2 (memory-source-tap [["a"]])]
    (with-expected-sink-sets [empty1 [], empty2 []]
      (test?<- src
               [!a]
               (src !a)
               (src !a)
               (:trap empty1))

      (test?<- src
               [!a]
               (src2 !a)
               (src2 !a)
               (:trap empty2)))))

(deftest test-trap
  (let [num [[1] [2]]]
    (with-expected-sink-sets [trap1 [[1]]]
      (test?<- [[2]]
               [?n]
               (num ?n)
               (odd-fail ?n)
               (:trap trap1)))

    (is (thrown? Exception (test?<- [[2]]
                                    [?n]
                                    (num ?n)
                                    (odd-fail ?n))))))
(deftest test-cascalog-tap-trap
  (let [num [[1] [2]]]
    (with-expected-sink-sets [trap1 [[1]]]
      (test?<- [[2]]
               [?n]
               (num ?n)
               (odd-fail ?n)
               (:trap (cascalog-tap nil trap1))))

    (is (thrown? Exception (test?<- [[2]]
                                    [?n]
                                    (num ?n)
                                    (odd-fail ?n))))))

(deftest test-trap-joins
  (let [age    [["A" 20] ["B" 21]]
        gender [["A" "m"] ["B" "f"]]]
    (with-expected-sink-sets [trap1 [[21]]
                              trap2 [[21 "f"]]]
      (test?<- [["A" 20 "m"]]
               [?p ?a ?g]
               (age ?p ?a)
               (gender ?p ?g)
               (odd-fail ?a)
               (:trap trap1))

      (test?<- [["A" 20 "m"]]
               [?p ?a ?g]
               (age ?p ?a)
               (gender ?p ?g)
               (odd-fail ?a ?g)
               (:trap trap2)))))
(deftest test-multi-trap
  (let [age [["A" 20] ["B" 21]]
        weight [["A" 191] ["B" 192]]]
    (with-expected-sink-sets [trap1 [[21]]
                              trap2 [["A" 20 191]] ]
      (let [sq (<- [?p ?a]
                   (age ?p ?a)
                   (odd-fail ?a)
                   (:trap trap1)
                   (:distinct false))]
        (test?<- []
                 [?p ?a ?w]
                 (sq ?p ?a)
                 (weight ?p ?w)
                 (odd-fail ?w ?p ?a)
                 (:trap trap2))))))
