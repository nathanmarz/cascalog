(ns cascalog.cascading-api-test
  (:use clojure.test
        [midje sweet cascalog]
        cascalog.logic.testing
        cascalog.cascading.testing
        cascalog.api)
  (:require [cascalog.logic.ops :as c]
            [cascalog.cascading.io :as io]
            [cascalog.logic.def :as d]
            [cascalog.cascading.def :as cd]
            [cascalog.cascading.stats :as stats])
  (:import [cascading.tuple Fields]
           [cascalog.test KeepEven OneBuffer CountAgg SumAgg]
           [cascalog.ops IdentityBuffer]
           [cascading.operation.text DateParser]))

(use-fixtures :once
  (fn [f]
    (set-cascading-platform!)
    (f)))

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

;; Testing Cascading Taps and Traps
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

(deftest test-cascalog-tap-source-and-sink
  (with-expected-sinks [sink1 [[4]]]
    (let [tap (cascalog-tap [[3]] sink1)]
      (?<- tap [?n]
           (tap ?raw)
           (inc ?raw :> ?n)))))
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

"TODO: These need union and combine to do proper renames."
(defn run-union-combine-tests
  "Runs a series of tests on the union and combine operations. v1,
  v2 and v3 must produce

    [[1] [2] [3]]
    [[3] [4] [5]]
    [[2] [4] [6]]"
  [v1 v2 v3]
  (test?- [[1] [2] [3] [4] [5]]                 (union v1 v2)
          [[1] [2] [3] [4] [5] [6]]             (union v1 v2 v3)
          [[3] [4] [5]]                         (union v2)
          [[1] [2] [3] [2] [4] [6]]             (combine v1 v3)
          [[1] [2] [3] [3] [4] [5] [2] [4] [6]] (combine v1 v2 v3)))

(deftest test-vector-union-combine
  (run-union-combine-tests [[1] [2] [3]]
                           [[3] [4] [5]]
                           [[2] [4] [6]]))

(deftest test-query-union-combine
  (run-union-combine-tests (<- [?v] ([[1] [2] [3]] ?v))
                           (<- [?v] ([[3] [4] [5]] ?v))
                           (<- [?v] ([[2] [4] [6]] ?v))))

(deftest test-cascading-union-combine
  (let [v1 [[1] [2] [3]]
        v2 [[3] [4] [5]]
        v3 [[2] [4] [6]]
        e1 []]
    (run-union-combine-tests v1 v2 v3)

    "Can't use empty taps inside of a union or combine."
    (is (thrown? IllegalArgumentException (union e1)))
    (is (thrown? IllegalArgumentException (combine e1)))))

(deftest test-sample-count
  "sample should return a number of samples equal to the specified
     sample size param"
  (let [numbers [[1] [2] [3] [4] [5] [6] [7] [8] [9] [10]]
        sampling-query (c/fixed-sample numbers 5)]
    (test?<- [[5]]
             [?count]
             (sampling-query ?s)
             (c/count ?count))))

(deftest test-sample-contents
  (let [numbers [[1 2] [3 4] [5 6] [7 8] [9 10]]
        sampling-query (c/fixed-sample numbers 5)]
    (fact "sample should contain some of the inputs"
          sampling-query => (produces-some [[1 2] [3 4] [5 6]]))))

(deftest select-fields-supports-cascalogtap
  (let [data (memory-source-tap ["f1" "f2" "f3" "f4"]
                                [[1 2 3 4] [11 12 13 14] [21 22 23 24]])
        cascalog-tap (cascalog-tap data nil)]
    (test?<- [[4 2] [14 12] [24 22]]
             [?a ?b]
             ((select-fields cascalog-tap ["f4" "f2"]) ?a ?b))))

(defn mk-agg-test-tuples []
  (-> (take 10 (iterate (fn [[a b]] [(inc a) b]) [0 1]))
      (vec)
      (conj [0 4])))

(defn mk-agg-test-results []
  (-> (take 9 (iterate (fn [[a b c]]
                         [(inc a) b c])
                       [1 1 1]))
      (vec)
      (conj [0 5 2])))

(deftest test-complex-agg-more-than-spill-threshold
  (let [num (mk-agg-test-tuples)]
    (test?<- (mk-agg-test-results)
             [?n ?s ?c]
             (num ?n ?v)
             (:spill-threshold 3)
             (c/sum ?v :> ?s)
             (c/count ?c))))

(deftest test-java-aggregator
  (let [vals [["a" 1] ["a" 2] ["b" 3] ["c" 8] ["c" 13] ["b" 1] ["d" 5] ["c" 8]]]
    (test?<- [["a" 2] ["b" 2] ["c" 3] ["d" 1]]
             [?f1 ?o]
             (vals ?f1 _)
             ((CountAgg.) :> ?o))

    (test?<- [["a" 3 2] ["b" 4 2] ["c" 29 3] ["d" 5 1]]
             [?key ?sum ?count]
             (vals ?key ?val)
             ((CountAgg.) ?count)
             ((SumAgg.) ?val :> ?sum))))

(deftest test-function-sink
  (let [pairs [[1 2] [2 10]]
        double-second-sink (fn [sq]
                             [[[1 2 4] [2 10 20]]
                              (<- [?a ?b ?c]
                                  (sq ?a ?b)
                                  (* 2 ?b :> ?c)
                                  (:distinct false)) ])]
    (test?- double-second-sink pairs)))

(defn sum-plus [a]
  (d/bufferop
   (cd/prepfn
    [_ _]
    (let [x (* 3 a)]
      {:operate (fn [tuples]
                  [(apply + x (map first tuples))])}))))

(deftest test-hof-ops
  (let [integer [[1] [2] [6]]]
    (test?<- [[72]]
             [?n]
             (integer ?v)
             ((sum-plus 21) ?v :> ?n))))

(deftest test-outfields-tap
  (is (thrown? AssertionError
               (get-out-fields (memory-source-tap Fields/ALL []))))
  (is (= ["!age"]
         (get-out-fields (memory-source-tap ["!age"] []))))
  (is (= ["?age" "field2"]
         (get-out-fields (memory-source-tap ["?age" "field2"] [])))))

;; ## Stats Interface Tests

(deftest stats-test
  (let [stats (atom nil)
        square (mapfn [x]
                      (stats/inc-by! "counter" x)
                      (* x x))]
    (io/with-tmp-files [path (io/unique-tmp-file "stats")]
      (??<- [?x ?y]
            ([1 2 3] ?x)
            (:name "StatsTestJob")
            (:stats-fn (comp
                        (stats/clojure-file path)
                        #(reset! stats %)))
            (square ?x :> ?y))

      (is (= "StatsTestJob" (:name @stats))
          "The name keyword sets the job's name.")

      (is (:successful? @stats) "The job's successful!")

      (is (= 6 (-> @stats :counters (get-in [stats/default-group "counter"])))
          "The counter is equal to (+ 1 2 3) = 6.")

      (is (= @stats (read-string (slurp path)))
          "stats/clojure-file writes the stats out to the temporary
          path. Slurping it back up gives you the same data structure
          as is in the atom."))))
