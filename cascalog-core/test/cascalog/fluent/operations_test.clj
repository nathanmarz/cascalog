(ns cascalog.fluent.operations-test
  (:use clojure.test
        cascalog.fluent.operations
        cascalog.fluent.types
        cascalog.fluent.flow
        cascalog.fluent.tap
        cascalog.fluent.cascading
        cascalog.fluent.def
        [midje sweet])
  (:require [cascalog.fluent.io :as io]))

(defn square [x]
  (* x x))

(defn sum [& xs]
  (reduce + xs))

(defmapfn plus-two [x]
  (+ 2 x))

(defn times
  [y]
  (mapfn [x] (* x y)))

(deftest custom-op-test
  (facts
    "Normal squaring function works."
    (square 10) => 100

    "And still works as a mapop. The behavior is unchanged."
    ((mapop* square) 3) => 9

    "anonymous mapops work as functions"
    ((mapfn [x] (* x 5)) 4) => 20

    "operations defined with def*fn work as normal functions."
    (plus-two 2) => 4

    "Higher order mapfns work normally"
    ((times 2) 4) => 8))

(deftest map-test
  (let [src (-> [["jenna" 10]
                 ["sam" 2]
                 ["oscar" 3]]
                (rename* ["user" "tweet-count"]))]
    (facts
      "Naming input and output the same replaces the input variable in
      the flow."
      (-> src
          (map* square "tweet-count" "tweet-count")
          to-memory)
      =>  [["jenna" 100]
           ["sam" 4]
           ["oscar" 9]]

      "Using a new name appends the variable."
      (-> src (map* square "tweet-count" "squared") to-memory)
      =>  [["jenna" 10 100]
           ["sam" 2 4]
           ["oscar" 3 9]])))

(deftest op-wrapper-test
  (fact
    "Higher order functions and vanilla functions
    lifted to mapops!"
    (-> [1 2 3 4 5]
        (rename* "a")
        (exec* (mapop* square) "a" "squared")
        (exec* (times 10) "a" "b")
        (to-memory))
    => [[1 1 10]
        [2 4 20]
        [3 9 30]
        [4 16 40]
        [5 25 50]]

    "Any function defined with def*op can be used directly in the
     flow:"
    (-> [1 2 3 4 5]
        (rename* "a")
        (exec* plus-two "a" "b")
        to-memory)
    => [[1 3] [2 4] [3 5] [4 6] [5 7]]))

(future-fact
 "Check that intermediates actually write out to their sequencefiles.")

(deftest check-intermediate-write
  (io/with-fs-tmp [_ tmp-a tmp-b]
    (fact "Interspersing calls to write doesn't affect the flow."
      (-> [[1 2] [2 3] [3 4] [4 5]]
          (rename* ["a" "b"])
          (write* (hfs-textline tmp-a))
          (map* #'inc "a" "inc")
          (filter* odd? "a")
          (map* square "inc" "squared")
          (map* dec "squared" "decreased")
          (write* (hfs-textline tmp-b))
          to-memory)
      => [[1 2 2 4 3] [3 4 4 16 15]])))

(deftest duplicate-inputs
  (io/with-fs-tmp [_ tmp-a tmp-b]
    (fact
      "duplicate inputs are okay, provided you sanitize them using
       with-dups. Note that with-dups won't currently clean up the
       extra delta vars for you. Fix this later by cleaning up the
       delta variables."
      (-> [[1 2] [2 3] [3 4] [4 5]]
          (rename* ["a" "b"])
          (with-dups ["a" "a" "b"]
            (fn [flow input delta]
              (-> flow (map* sum input "summed"))))
          (write* (hfs-textline tmp-a))
          (graph tmp-b)
          (to-memory))
      => [[1 2 1 4]
          [2 3 2 7]
          [3 4 3 10]
          [4 5 4 13]])))

(deftest test-merged-flow
  (let [source (-> [[1 1] [2 2] [3 3] [4 4]]
                   (rename* ["a" "b"]))
        a      (-> source
                   (map* square "a" "c")
                   (map* dec "b" "d"))
        b      (-> source
                   (map* inc "a" "c")
                   (map* inc "b" "d"))]
    (fact "Merge combines streams without any join. This test forks a
           source, applies operations to each branch then merges the
           branches back together again."
          (sort (-> (merge* a b)
                    (map* str "d" "e")
                    to-memory)) => (sort [[1 1 1 0 "0"]
                                          [2 2 4 1 "1"]
                                          [3 3 9 2 "2"]
                                          [4 4 16 3 "3"]
                                          [1 1 2 2 "2"]
                                          [2 2 3 3 "3"]
                                          [3 3 4 4 "4"]
                                          [4 4 5 5 "5"]]))))

(deffilterfn gt2
  [x] (> x 2))

(def sum (partial parallel-agg +))

(deftest test-co-group
  (let [source (-> (generator [[1 1] [2 2] [3 3] [4 4]]))
        a      (-> source
                   (rename* ["a" "b"])
                   (filter* gt2 "a")
                   (map* square "b" "c")
                   ;(discard* "b")
                   )
        b      (-> source
                   (rename* ["x" "y"]))]
    (fact "Join joins stuff"
          (sort (-> (co-group* [a b] [["a"] ["x"]] ["a" "x" "b" "c" "y"]  [])
                    (map* str "y" "q")
                    to-memory)) => (sort [[3 3 9 3 3 "3"]
                                          [4 4 16 4 4 "4"]]))
    (fact "Agg after join"
          (let [a (-> (generator [[1 1] [1 2] [2 2]]) (rename* ["a" "b"]))
                b (-> (generator [[1 10] [2 15]]) (rename* ["x" "y"]))
                q (co-group* [a b] [["a"] ["x"]] nil [(sum "y" "s")])]
            (fact (to-memory q) => [[1 1 20] [2 2 15]])
            ))))
