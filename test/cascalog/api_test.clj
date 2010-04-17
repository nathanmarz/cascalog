(ns cascalog.api-test
  (:use clojure.test
        cascalog.testing
        cascalog.api)
  (:import [cascading.tuple Fields])
  (:require [cascalog [workflow :as w] [ops :as c]]))

(deftest test-simple-query
  (with-tmp-sources [age [["n" 24] ["n" 23] ["i" 31] ["c" 30] ["j" 21] ["q" nil]]]
     (test?<- [["j"] ["n"]] [?p] (age ?p ?a) (< ?a 25))
     (test?<- [["j"] ["n"] ["n"]] {:distinct false} [?p] (age ?p ?a) (< ?a 25))
    ))

(deftest test-larger-tuples
  (with-tmp-sources [stats [["n" 6 190 nil] ["n" 6 195 nil] ["i" 5 180 31] ["g" 5 150 60]]
                     friends [["n" "i" 6] ["n" "g" 20] ["g" "i" nil]]]
     (test?<- [["g" 60]] [?p ?a] (stats ?p _ _ ?a) (friends ?p _ _))
     (test?<- [] [?p ?a] (stats ?p 1000 _ ?a))
     (test?<- [["n"] ["n"]] {:distinct false} [?p] (stats ?p _ _ nil))
    ))

(deftest test-multi-join
  (with-tmp-sources [age [["n" 24] ["a" 15] ["j" 24] ["d" 24] ["b" 15] ["z" 62] ["q" 24]]
                     friends [["n" "a" 16] ["n" "j" 12] ["j" "n" 10] ["j" "d" nil]
                              ["d" "q" nil] ["b" "a" nil] ["j" "a" 1] ["a" "z" 1]]]
        (test?<- [["n" "j"] ["j" "n"] ["j" "d"] ["d" "q"] ["b" "a"]]
          [?p ?p2] (age ?p ?a) (age ?p2 ?a) (friends ?p ?p2 _))
        ))

(deftest test-many-joins
  (with-tmp-sources [age-prizes [[10 "toy"] [20 "animal"] [30 "car"] [40 "house"]]
                     friends    [["n" "j"] ["n" "m"] ["n" "a"] ["j" "a"] ["a" "z"]]
                     age         [["z" 20] ["a" 10] ["n" 15]]]
        (test?<- [["n" "animal-!!"] ["n" "house-!!"] ["j" "house-!!"]]
          [?p ?prize] (friends ?p ?p2) (friends ?p2 ?p3) (age ?p3 ?a)
                          (* 2 ?a :> ?a2) (age-prizes ?a2 ?prize2) (str ?prize2 "-!!" :> ?prize))
        ))

(deftest test-bloated-join
  (with-tmp-sources [gender     [["n" "male"] ["j" "male"] ["a" nil] ["z" "female"]]
                     interest   [["n" "bball"] ["n" "dl"] ["j" "tennis"] ["z" "stuff"] ["a" "shoes"]]
                     friends    [["n" "j"] ["n" "m"] ["n" "a"] ["j" "a"] ["a" "z"] ["z" "a"]]
                     age        [["z" 20] ["a" 10] ["n" 15]]]
        (test?<- [["n" "bball" 15 "male"] ["n" "dl" 15 "male"]
                  ["a" "shoes" 10 nil] ["z" "stuff" 20 "female"]]
          [!p !interest !age !gender] (friends !p _) (age !p !age) (interest !p !interest) (gender !p !gender))
        ))

(w/defmapcatop split [#^String words]
  (seq (.split words "\\s+")))

(deftest test-countall
  (with-tmp-sources [sentence [["hello this is a"] ["say hello hello to the man"] ["this is the cool beans man"]]]
    (test?<- [["hello" 3] ["this" 2] ["is" 2]
              ["a" 1] ["say" 1] ["to" 1] ["the" 2]
              ["man" 2] ["cool" 1] ["beans" 1]]
          [?w ?c] (sentence ?s) (split ?s :> ?w) (c/count ?c))
    ))

(deftest test-multi-sink)

(deftest test-multi-agg
  (with-tmp-sources [value [["a" 1] ["a" 2] ["b" 10] ["c" 3] ["b" 2] ["a" 6]] ]
    (test?<- [["a" 12] ["b" 14] ["c" 4]] [?v ?a] (value ?v ?n) (c/count ?c) (c/sum ?n :> ?s) (+ ?s ?c :> ?a))
    ))


(deftest test-joins-aggs
  (with-tmp-sources [friend [["n" "a"] ["n" "j"] ["n" "q"] ["j" "n"] ["j" "a"] ["j" "z"] ["z" "t"]]
                     age    [["n" 25] ["z" 26] ["j" 20]] ]
    (test?<- [["n"] ["j"]] [?p] (age ?p _) (friend ?p _) (c/count ?c) (> ?c 2))
    ))

(deftest test-global-agg
   (with-tmp-sources [num [[1] [2] [5] [6] [10] [12]] ]
     (test?<- [[6]] [?c] (num _) (c/count ?c))
     (test?<- [[6 72]] [?c ?s2] (num ?n) (c/count ?c) (c/sum ?n :> ?s) (* 2 ?s :> ?s2))
  ))

(w/defaggregateop evens-vs-odds
  ([] 0)
  ([context val] (if (odd? val) (dec context) (inc context)))
  ([context] [context]))

(deftest test-complex-noncomplex-agg-mix
  (with-tmp-sources [num [["a" 1] ["a" 2] ["a" 5] ["c" 6] ["d" 9] ["a" 12] ["c" 16] ["e" 16]] ]
     (test?<- [["a" 4 0 20] ["c" 2 2 22] ["d" 1 -1 9] ["e" 1 1 16]]
       [?a ?c ?e ?s] (num ?a ?n) (c/count ?c) (c/sum ?n :> ?s) (evens-vs-odds ?n :> ?e))
  ))

(defn mk-agg-test-tuples []
  (conj (vec (take 12000 (iterate (fn [[a b]] [(inc a) b]) [0 1]))) [0 10]))

(defn mk-agg-test-results []
  (conj (vec (take 11999 (iterate (fn [[a b c]] [(inc a) b c]) [1 1 1]))) [0 11 2]))

(deftest test-complex-agg-more-than-10000
  (with-tmp-sources [num (mk-agg-test-tuples)]
     (test?<- (mk-agg-test-results)
       [?n ?s ?c] (num ?n ?v) (c/sum ?v :> ?s) (c/count ?c))))

(deftest test-multi-rule
  (with-tmp-sources [age [["n" 24] ["c" 40] ["j" 23] ["g" 50]]
                     interest [["n" "bb" nil] ["n" "fb" 20] ["g" "ck" 30] ["j" "nz" 10] ["j" "hk" 1] ["jj" "ee" nil]]
                     follows [["n" "j"] ["j" "n"] ["j" "a"] ["n" "a"] ["g" "q"]] ]
      (let [many-follow (<- [?p] (follows ?p _) (c/count ?c) (> ?c 1))
            active-follows (<- [?p ?p2] (many-follow ?p) (many-follow ?p2) (follows ?p ?p2))
            unknown-interest (<- [?p] (age ?p ?a) (interest ?p _ !i) (nil? !i))
            weird-follows (<- [?p ?p2] (active-follows ?p ?p2) (unknown-interest ?p2))]
          (test?- [["n" "j"] ["j" "n"]] active-follows
                  [["j" "n"]] weird-follows
                  [["n"]] unknown-interest)
        )))

(deftest test-only-complex-agg)

(deftest test-only-noncomplex-agg)

(deftest test-only-one-buffer)

(deftest test-error-on-lacking-output-fields)