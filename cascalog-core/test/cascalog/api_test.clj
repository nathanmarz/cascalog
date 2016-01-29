(ns cascalog.api-test
  (:use clojure.test
        [midje sweet cascalog]
        cascalog.logic.testing
        cascalog.cascading.testing
        cascalog.in-memory.testing
        cascalog.api)
  (:require [cascalog.logic.ops :as c]
            [cascalog.logic.def :as d]))

(use-fixtures :once
  (fn [f]
    (set-cascading-platform!)
    (f)
    (set-in-memory-platform!)
    (f)))

(defmapfn mk-one
  "Returns 1 for any input."
  [& tuple] 1)

(deftest test-no-input
  (let [nums [[1] [2] [3]]]
    (test?<- [[1 1] [2 1] [3 1]]
             [?n ?n2]
             (nums ?n)
             (mk-one :> ?n2))
    (test?<- [[1 1] [1 2] [1 3]
              [2 1] [2 2] [2 3]
              [3 1] [3 2] [3 3]]
             [?n ?n3]
             (nums ?n)
             (mk-one :> ?n2)
             (nums ?n3))))

(deftest test-empty-vector-input
  (let [empty-vector []]
    (test?<- []
             [?a]
             (empty-vector ?a))))

(deftest test-simple-query
  (let [age [["n" 24] ["n" 23] ["i" 31] ["c" 30] ["j" 21] ["q" nil]]]
    (test?<- [["j"] ["n"]]
             [?p]
             (age ?p ?a)
             (< ?a 25)
             (:distinct true))
    (test?<- [["j"] ["n"] ["n"]]
             [?p]
             (age ?p ?a)
             (< ?a 25))))

(deftest test-larger-tuples
  (let [stats [["n" 6 190 nil] ["n" 6 195 nil]
               ["i" 5 180 31] ["g" 5 150 60]]
        friends [["n" "i" 6] ["n" "g" 20]
                 ["g" "i" nil]]]
    (test?<- [["g" 60]]
             [?p ?a]
             (stats ?p _ _ ?a)
             (friends ?p _ _))
    (test?<- []
             [?p ?a]
             (stats ?p 1000 _ ?a))
    (test?<- [["n"] ["n"]]
             [?p]
             (stats ?p _ _ nil)
             (:distinct false))))

(deftest test-multi-join
  (let [age [["n" 24] ["a" 15] ["j" 24]
             ["d" 24] ["b" 15]
             ["z" 62] ["q" 24]]
        friends [["n" "a" 16] ["n" "j" 12]
                 ["j" "n" 10] ["j" "d" nil]
                 ["d" "q" nil] ["b" "a" nil]
                 ["j" "a" 1] ["a" "z" 1]]]
    (test?<- [["n" "j"] ["j" "n"]
              ["j" "d"] ["d" "q"] ["b" "a"]]
             [?p ?p2]
             (age ?p ?a)
             (age ?p2 ?a)
             (friends ?p ?p2 _))))

(deftest test-many-joins
  (let [age-prizes [[10 "toy"] [20 "animal"]
                    [30 "car"] [40 "house"]]
        friends    [["n" "j"] ["n" "m"]
                    ["n" "a"] ["j" "a"]
                    ["a" "z"]]
        age         [["z" 20] ["a" 10]
                     ["n" 15]]]
    (test?<- [["n" "animal-!!"] ["n" "house-!!"]
              ["j" "house-!!"]]
             [?p ?prize]
             (friends ?p ?p2)
             (friends ?p2 ?p3)
             (age ?p3 ?a)
             (* 2 ?a :> ?a2)
             (age-prizes ?a2 ?prize2)
             (str ?prize2 "-!!" :> ?prize))))

(deftest test-bloated-join
  (let [gender     [["n" "male"] ["j" "male"]
                    ["a" nil] ["z" "female"]]
        interest   [["n" "bball"] ["n" "dl"] ["j" "tennis"]
                    ["z" "stuff"] ["a" "shoes"]]
        friends    [["n" "j"] ["n" "m"] ["n" "a"]
                    ["j" "a"] ["a" "z"] ["z" "a"]]
        age        [["z" 20] ["a" 10] ["n" 15]]]
    (test?<- [["n" "bball" 15 "male"] ["n" "dl" 15 "male"]
              ["a" "shoes" 10 nil] ["z" "stuff" 20 "female"]]
             [!p !interest !age !gender]
             (friends !p _)
             (age !p !age)
             (interest !p !interest)
             (gender !p !gender)
             (:distinct true))))

(defmapcatfn split [^String words]
  (seq (.split words "\\s+")))

(deftest test-countall
  (let [sentence [["hello this is a"]
                  ["say hello hello to the man"]
                  ["this is the cool beans man"]]]
    (test?<- :info [["hello" 3] ["this" 2] ["is" 2]
                    ["a" 1] ["say" 1] ["to" 1] ["the" 2]
                    ["man" 2] ["cool" 1] ["beans" 1]]
             [?w ?c]
             (sentence ?s)
             (split ?s :> ?w)
             (c/count ?c)
             (:name "test-countall"))))

(deftest test-multi-agg
  (let [value [["a" 1] ["a" 2] ["b" 10]
               ["c" 3] ["b" 2] ["a" 6]] ]
    (test?<- [["a" 12] ["b" 14] ["c" 4]]
             [?v ?a]
             (value ?v ?n)
             (c/count ?c)
             (c/sum ?n :> ?s)
             (+ ?s ?c :> ?a))))

(deftest test-joins-aggs
  (let [friend [["n" "a"] ["n" "j"] ["n" "q"]
                ["j" "n"] ["j" "a"]
                ["j" "z"] ["z" "t"]]
        age    [["n" 25] ["z" 26] ["j" 20]] ]
    (test?<- [["n"] ["j"]]
             [?p]
             (age ?p _)
             (friend ?p _)
             (c/count ?c)
             (> ?c 2))))

(deftest test-global-agg
  (let [num [[1] [2] [5] [6] [10] [12]]]
    (test?<- [[6]]
             [?c]
             (num _)
             (c/count ?c))
    (test?<- [[6 72]]
             [?c ?s2]
             (num ?n)
             (c/count ?c)
             (c/sum ?n :> ?s)
             (* 2 ?s :> ?s2))))

(defaggregatefn evens-vs-odds
  "Decrements state for odd inputs, increases for even. Returns final
   state as a 1-tuple."
  ([] 0)
  ([context val] (if (odd? val)
                   (dec context)
                   (inc context)))
  ([context] [context]))

(deftest test-complex-noncomplex-agg-mix
  (let [num [["a" 1] ["a" 2] ["a" 5]
             ["c" 6] ["d" 9] ["a" 12]
             ["c" 16] ["e" 16]] ]
    (test?<- [["a" 4 0 20] ["c" 2 2 22]
              ["d" 1 -1 9] ["e" 1 1 16]]
             [?a ?c ?e ?s]
             (num ?a ?n)
             (c/count ?c)
             (c/sum ?n :> ?s)
             (evens-vs-odds ?n :> ?e))))

(deftest test-multi-rule
  (let [age [["n" 24] ["c" 40] ["j" 23] ["g" 50]]
        interest [["n" "bb" nil] ["n" "fb" 20]
                  ["g" "ck" 30] ["j" "nz" 10]
                  ["j" "hk" 1] ["jj" "ee" nil]]
        follows [["n" "j"] ["j" "n"] ["j" "a"] ["n" "a"] ["g" "q"]]
        many-follow      (<- [?p]
                             (follows ?p _)
                             (c/count ?c)
                             (> ?c 1))
        active-follows   (<- [?p ?p2]
                             (many-follow ?p)
                             (many-follow ?p2)
                             (follows ?p ?p2))
        unknown-interest (<- [?p]
                             (age ?p ?a)
                             (interest ?p _ !i)
                             (nil? !i))
        weird-follows    (<- [?p ?p2]
                             (active-follows ?p ?p2)
                             (unknown-interest ?p2))]
    (test?- [["n" "j"] ["j" "n"]] active-follows
            [["j" "n"]]           weird-follows
            [["n"]]               unknown-interest)))

(deftest test-filter-same-field
  (let [nums [[1 1] [0 0] [1 2] [3 7] [8 64] [7 1] [2 4] [6 6]]]
    (test?<- [[1] [0] [6]]
             [?n]
             (nums ?n ?n))
    (test?<- [[1 1] [0 0] [8 64] [2 4]]
             [?n ?n2]
             (nums ?n ?n2)
             (* ?n ?n :> ?n2))
    (test?<- [[0]]
             [?n]
             (nums ?n ?n)
             (* ?n ?n :> ?n)
             (+ ?n ?n :> ?n))
    (test?<- [[1 1] [1 2] [0 0] [6 6]]
             [?n ?n2]
             (nums ?n ?n)
             (nums ?n ?n2))
    (test?<- [[14]]
             [?s]
             (nums ?n ?n)
             (* 2 ?n :> ?n2)
             (c/sum ?n2 :> ?s))
    (test?<- [[6] [0]]
             [?n2]
             (nums ?n ?n)
             (nums ?n2 ?n2)
             (* 6 ?n :> ?n2))))

(defbufferfn select-first [tuples]
  [(first tuples)])

(deftest test-sort
  (let [pairs [["a" 1] ["a" 2] ["a" 3] ["b" 10] ["b" 20]]]
    (test?<- [["a" 1] ["b" 10]]
             [?f1 ?f2]
             (pairs ?f1 ?v)
             (:sort ?v)
             (select-first ?v :> ?f2))

    (test?<- [["a" 3] ["b" 20]]
             [?f1 ?f2]
             (pairs ?f1 ?v)
             (:sort ?v)
             (:reverse true)
             (select-first ?v :> ?f2))))

(defn existence2str [obj]
  (if obj "some" "none"))

(defmapfn outer-join-tester [obj]
  (if obj "o" "n"))

(defmapfn outer-join-tester2 [obj]
  (if obj "o2" "n2"))

(defmapcatfn outer-join-tester3 [obj]
  (if obj [1] [1 1]))

(deftest test-outer-join-basic
  (let [person [["a"] ["b"] ["c"] ["d"]]
        follows [["a" "b"] ["c" "e"] ["c" "d"]]]
    (test?<- [["a" "b"] ["c" "e"] ["c" "d"]
              ["b" nil] ["d" nil]]
             [?p !!p2]
             (person ?p)
             (follows ?p !!p2))

    (test?<- [["a" "b" "b"] ["c" "e" "d"] ["c" "e" "e"]
              ["c" "d" "d"] ["c" "d" "e"] ["b" nil nil]
              ["d" nil nil]]
             [?p !!p2 !!p3]
             (person ?p)
             (follows ?p !!p2)
             (follows ?p !!p3))

    (test?<- [["a" 1 1] ["c" 2 2]
              ["b" 0 1] ["d" 0 1]]
             [?p ?c ?t]
             (person ?p)
             (follows ?p !!p2)
             (c/!count !!p2 :> ?c)
             (c/count ?t))

    (test?<- [["a" "some"] ["b" "none"]
              ["c" "some"] ["d" "none"]]
             [?p ?s]
             (person ?p)
             (follows ?p !!p2)
             (existence2str !!p2 :> ?s)
             (:distinct true))))

(deftest test-outer-join-complex
  (let [age [["a" 20] ["b" 30]
             ["c" 27] ["d" 40]]
        rec1 [["a" 1 2] ["b" 30 16] ["e" 3 4]]
        rec2 [["a" 20 6] ["c" 27 25]
              ["c" 1 11] ["f" 30 1]
              ["b" 100 16]]]
    (test?<- [["a" 20 1 2 6] ["c" 27 nil nil 25]
              ["d" 40 nil nil nil] ["b" 30 30 16 nil]]
             [?p ?a !!f1 !!f2 !!f3]
             (age ?p ?a)
             (rec1 ?p !!f1 !!f2)
             (rec2 ?p ?a !!f3)
             (:name "outer-join-complex"))))

(deftest test-outer-join-assertions
  (let [age [["a" 20] ["b" 30] ["c" 27] ["d" 40]]
        rec1 [["a" 1 2] ["b" 30 16] ["e" 3 4]]]

    "Each unground var can only appear in one generator."
    (thrown?<- IllegalArgumentException
               [!!a ?c]
               (age !!a ?b)
               (rec1 !!a ?f1 ?f2)
               (- ?b 2 :> ?c))

    "Ungrounding vars have to spring from a generator."
    (thrown?<- IllegalArgumentException
               [!!a !!c]
               (age !!a ?b)
               (- ?b 2 :> !!c))

    "No ungrounding vars allowed in generators-as-sets."
    (thrown?<- IllegalArgumentException
               [!!a]
               (age !!a ?b)
               (rec1 !!a _ _ :> true))

    (thrown?<- IllegalArgumentException
               [?a !!c]
               (age ?a ?b)
               (rec1 ?a _ _ :> !!c))))

(deftest test-full-outer-join
  (let [age        [["A" 20] ["B" 30] ["C" 27] ["D" 40]]
        gender     [["A" "m"] ["B" "f"] ["E" "m"] ["F" "f"]]
        follows    [["A" "B"] ["B" "E"] ["B" "G"] ["E" "D"]]
        age-tokens [["B" 20 "d"] ["A" 30 "a"]]]
    (test?<- [["A" 20 "m"] ["B" 30 "f"]
              ["C" 27 nil] ["D" 40 nil]
              ["E" nil "m"] ["F" nil "f"]]
             [?p !!a !!g]
             (age ?p !!a)
             (gender ?p !!g))

    (test?<- [["A" "o" "o2"] ["B" "o" "o2"]
              ["C" "o" "n2"] ["D" "o" "n2"]
              ["E" "n" "o2"] ["F" "n" "o2"]]
             [?p ?s ?s2]
             (age ?p !!a)
             (gender ?p !!g)
             (outer-join-tester !!a :> ?s)
             (outer-join-tester2 !!g :> ?s2))

    (test?<- [["A" 1] ["B" 1] ["C" 1]
              ["D" 1] ["E" 2] ["F" 2]]
             [?p ?c]
             (age ?p !!a)
             (gender ?p !!g)
             (outer-join-tester3 !!a :> ?t)
             (c/count ?c)
             (:name "count-test"))

    (test?<- [["A" "a"] ["E" nil]]
             [?p !!t]
             (follows ?p ?p2)
             (age ?p2 ?a2)
             (age-tokens ?p ?a2 !!t))

    (test?<- [["E"]]
             [?p]
             (follows ?p ?p2)
             (age ?p2 ?a2)
             (age-tokens ?p ?a2 !!t)
             (nil? !!t))))

(defn hof-add [a]
  "Adds the static variable `a` to dynamic input `n`."
  (mapfn [n] (+ a n)))

(defn hof-arithmetic [a b]
  (mapfn [n] (+ b (* a n))))

(deftest test-hof-ops
  (let [integer [[1] [2] [6]]]
    (test?<- [[4] [5] [9]]
             [?n]
             (integer ?v)
             ((hof-add 3) ?v :> ?n))

    (test?<- [[-5] [-4] [0]]
             [?n]
             (integer ?v)
             ((hof-add -6) ?v :> ?n))

    (test?<- [[3] [5] [13]]
             [?n]
             (integer ?v)
             ((hof-arithmetic 2 1) ?v :> ?n))))


(defn lala-appended [source]
  (let [outvars ["?a"]]
    (<- outvars
        (source ?line)
        (str ?line "lalala" :>> outvars)
        (:distinct false))))

(deftest test-dynamic-vars
  (let [sentence [["nathan david"] ["chicken"]]]
    (test?<- [["nathan davidlalala"] ["chickenlalala"]]
             [?out]
             ((lala-appended sentence) ?out))

    (test?<- [["nathan davida"] ["chickena"]]
             [?out]
             (sentence :>> [?line])
             (str :<< ["?line" "a"] :>> ["?out"]))))

(defbufferfn nothing-buf [tuples] tuples)

(deftest test-outer-join-anon
  (let [person  [["a"] ["b"] ["c"]]
        follows [["a" "b" 1] ["c" "e" 2] ["c" "d" 3]]]
    (test?<- [["a" "b"] ["c" "e"]
              ["c" "d"] ["b" nil]]
             [?p !!p2]
             (person ?p)
             (follows ?p !!p2 _))))

(defbufferfn some-op [tuples]
  (partition 2 1 tuples))

(deftest test-buffer-leak
  "Makes sure that the iterator created off of Iterator<TupleEntry>
  doesn't get its state switched out from under it.

  https://github.com/nathanmarz/cascalog/issues/251"
  (let [test-data [["u1", 1, "p1"]
                   ["u1", 3, "p2"]
                   ["u1", 5, "p1"]
                   ["u1", 8, "p2"]]]
    (<- [?user ?first ?second]
        (test-data :> ?user ?timeindex ?p)
        (:sort ?timeindex)
        (some-op :< ?p ?timeindex :> ?first ?second))
    => (produces [["u1" ["p1" 1] ["p2" 3]]
                  ["u1" ["p2" 3] ["p1" 5]]
                  ["u1" ["p1" 5] ["p2" 8]]])))

(defbufferfn some-op2 [tuples] (prn (vec tuples)) (partition 2 1 tuples))

(deftest test-negate-join
  (let [left  [["a" 1]
               ["b" 2]]
        right [["b"]]]
    (future-fact "Join negation"
                 (<- [?x ?y]
                     (left ?x ?y)
                     (right ?x :> false)) => (produces [["a" 1]]))))

(defbufferiterfn itersum [tuples-iter]
  [(->> (iterator-seq tuples-iter)
        (map first)
        (reduce +))])

(deftest test-bufferiter
  (let [nums [[1] [2] [4]]]
    (test?<- [[7]]
             [?s]
             (nums ?n)
             (itersum ?n :> ?s))))

(defn inc-tuple [& tuple]
  (map inc tuple))




(deftest test-avg
  (let [num1 [[1] [2] [3] [4] [10]]
        pair [["a" 1] ["a" 2] ["a" 3] ["b" 3]]]
    (test?<- [[4]]
             [?avg]
             (num1 ?n)
             (c/avg ?n :> ?avg)
             (:name "test-avg-1"))

    (test?<- [["a" 2] ["b" 3]]
             [?l ?avg]
             (pair ?l ?n)
             (c/avg ?n :> ?avg))))

(deftest test-distinct-count
  (let [num1 [[1] [2] [2] [4] [1] [6] [19] [1]]
        pair [["a" 1] ["a" 2] ["b" 3] ["a" 1]]]
    (test?<- [[5]]
             [?c]
             (num1 ?n)
             (c/distinct-count ?n :> ?c))

    (test?<- [["a" 2] ["b" 1]]
             [?l ?c]
             (pair ?l ?n)
             (c/distinct-count ?n :> ?c))))

(deftest test-nullable-agg
  (let [follows [["a" "b"] ["b" "c"] ["a" "c"]]]
    (test?<- [["a" 2] ["b" 1]]
             [?p !c]
             (follows ?p _)
             (c/count !c))))

(deffilterfn a-fail [n & all]
  (if (= "a" n)
    (throw (RuntimeException.)) true))

(defn multipagg-init [v1 v2 v3]
  [v1 (+ v2 v3)])

(defn multipagg-combiner [v1 v2 v3 v4]
  [(+ v1 v3)
   (* v2 v4)])

(defparallelagg multipagg
  :init-var #'multipagg-init
  :combine-var #'multipagg-combiner)

(defaggregatefn slow-count
  ([] 0)
  ([context val] (inc context))
  ([context] [context]))

(deftest test-multi-parallel-agg
  (let [vals [[1 2 3] [4 5 6] [7 8 9]]]
    (test?<- [[12 935 3]]
             [?d ?e ?count]
             (vals ?a ?b ?c)
             (multipagg ?a ?b ?c :> ?d ?e)
             (c/count ?count))

    (test?<- [[12 935 3]]
             [?d ?e ?count]
             (vals ?a ?b ?c)
             (multipagg ?a ?b ?c :> ?d ?e)
             (slow-count ?c :> ?count))))

(deftest test-keyword-args
  (test?<- [[":onetwo"]]
           [?b]
           ([["two"]] ?a)
           (str :one ?a :> ?b))

  (test?<- [["face"]]
           [?a]
           ([["face" :cake]] ?a :cake)))

(deftest test-complex-constraints
  (let [pairs [[1 2] [2 4] [3 3]]
        double-times (mapfn [x y] [(* 2 x) y])]
    "Both output variables must be equal."
    (test?<- [[1 2] [2 4]]
             [?a ?b]
             (double-times ?a ?b :> ?b ?b)
             (pairs ?a ?b))

    "Function guard on a source, and a function guard on the
    operation's output."
    (test?<- [[2]]
             [?b]
             (* ?b 3 :> even?)
             (pairs odd? ?b))))

(deftest test-constant-substitution
  (let [pairs [[1 2] [1 3] [2 5]]]
    (test?<- [[1 2]]
             [?a ?b]
             (pairs ?a ?b)
             (* 2 ?b :> 4)
             (:distinct false))

    (test?<- [[1]]
             [?a]
             (pairs ?a ?b)
             (c/count 2))

    (test?<- [[2]] [?a]
             (pairs ?a _)
             (odd? ?a :> false))

    (future-fact
     "Constants should work in aggregator predicates. See
      https://github.com/nathanmarz/cascalog/issues/26"
     (<- [?a ?count]
         (pairs ?a _)
         (c/sum 2 :> ?count)) => (produces [[1 4] [2 2]]))))

(defmulti multi-test class)
(defmethod multi-test String [x] "string!")
(defmethod multi-test Long [x] "number!")
(defmethod multi-test Integer [x] "number!")
(defmethod multi-test Double [x] "double!")

(deftest test-multimethod-support
  (let [src [["word."] [1] [1.0]]]
    (test?<- [["string!"] ["number!"] ["double!"]]
             [?result]
             (src ?thing)
             (multi-test ?thing :> ?result))))


(defn var-apply [v]
  "Applies the supplied var v to the supplied `xs`."
  (mapfn [& xs] (apply v xs)))

(deftest test-var-constants
  (let [coll-src [[[3 2 4 1]]
                  [[1 2 3 4 5]]]
        num-src  [[1 2] [3 4]]]
    "Each tuple in source is a vector; the sum of each vector
             should be reflected in the output."
    (test?<- [[10] [15]]
             [?sum]
             (coll-src ?coll)
             (reduce #'+ ?coll :> ?sum))

    "Operation parameters can be vars or anything kryo serializable."
    (test?<- [[1 2 2] [3 4 12]]
             [?x ?y ?z]
             (num-src ?x ?y)
             ((var-apply *) ?x ?y :> ?z))

    "Regexes are serializable w/ Kryo."
    (test?<- [["a" "b"]]
             [?a ?b]
             ([["a\tb"]] ?s)
             ((c/re-parse #"[^\s]+") :< ?s :> ?a ?b))))

(def bob-set
  (d/filterop #{"bob"}))

(deftest test-ifn-implementers
  (let [people [["bob"] ["sam"]]]
    (fact "A set can be used as a predicate op, provided it's bound
           to a var."
          (<- [?person]
              (people ?person)
              (bob-set ?person)) => (produces [["bob"]]))))

(deftest test-filter-capture
  (let [src [[1] [2]]]

    (fact
     "The result of filterops can be captured."
     (<- [?x ?z]
         (src ?x)
         ((d/filterop odd?) ?x :> ?z)) => (produces [[1 true] [2 false]]))

    (fact
     "You can filter against the output of a filter too using a
     literal:"
     (<- [?x]
         (src ?x)
         ((d/filterop odd?) ?x :> false)) => (produces [[2]])

     ", OR with a function:"
     (<- [?x]
         (src ?x)
         ((d/filterop odd?) ?x :> false?)) => (produces [[2]]))

    (fact "mapops can be used as filters if there are no output
    variables:"
          (<- [?x]
              (src ?x)
              ((d/mapop even?) ?x)) => (produces [[2]]))))

(future-fact "test outer join with functions.")

(future-fact "test mongo"
             "function required for join"
             "2 inner join, 2 outer join portion"
             "functions -> joins -> functions -> joins"
             "functions that run after outer join"
             "no aggregator")

(future-fact "Test only complex aggregators.")
(future-fact "Test only non-complex aggregators.")
(future-fact "test only one buffer.")
(future-fact "Test error on missing output fields.")

;; TODO: Fix select-fields... maybe project is busted.
(deftest test-select-fields-query
  (let [wide [[1 2 3 4 5 6]]
        sq (<- [!f1 !f4 !f5 ?f6]
               (wide !f1 !f2 !f3 !f4 !f5 ?f6)
               (:distinct false))]
    (test?- [[1]]     (select-fields sq "!f1"))
    (test?- [[1 6]]   (select-fields sq ["!f1" "?f6"]))
    (test?- [[1 6]]   (select-fields sq ["!f1" "?f6"]))
    (test?- [[5 4 6]] (select-fields sq ["!f5" "!f4" "?f6"]))))

(deftest test-limit
  (let [pair [["a" 1] ["a" 3] ["a" 2]
              ["a" 4] ["b" 1] ["b" 6]
              ["b" 7] ["c" 0]]]
    (test?<- [[0] [1] [1] [2]
              [3] [4] [6] [7]]
             [?n2]
             (pair _ ?n)
             (:sort ?n)
             (nothing-buf ?n :> ?n2))

    (test?<- [[0] [1]]
             [?n2]
             (pair _ ?n)
             (:sort ?n)
             ((c/limit 2) ?n :> ?n2))

    (test?<- [[1 1] [2 2] [3 3]
              [4 4] [1 5]]
             [?n2 ?r]
             (pair ?l ?n)
             (:sort ?l ?n)
             ((c/limit-rank 5) ?n :> ?n2 ?r))

    (test?<- [["c" 0] ["b" 7]]
             [?l2 ?n2]
             (pair ?l ?n)
             (:sort ?l ?n)
             (:reverse true)
             ((c/limit 2) ?l ?n :> ?l2 ?n2))

    (test?<- [[0] [1] [1]]
             [?n2]
             (pair _ ?n)
             (:sort ?n)
             ((c/limit 3) ?n :> ?n2))

    (test?<- [[0 1] [1 2] [1 3]]
             [?n2 ?r]
             (pair _ ?n)
             (:sort ?n)
             ((c/limit-rank 3) ?n :> ?n2 ?r))

    (test?<- [[6] [7]]
             [?n2]
             (pair _ ?n)
             (:sort ?n)
             (:reverse true)
             ((c/limit 2) ?n :> ?n2))

    (test?<- [[6 2] [7 1]]
             [?n2 ?r]
             (pair _ ?n)
             (:sort ?n)
             (:reverse true)
             ((c/limit-rank 2) ?n :> ?n2 ?r))

    (test?<- [["a" 1] ["a" 2] ["b" 1]
              ["b" 6] ["c" 0]]
             [?l ?n2]
             (pair ?l ?n)
             (:sort ?n)
             ((c/limit 2) ?n :> ?n2))))

(deftest vector-args-should-work
  (let [data [[{:a {:b 2}}]]]
    (test?<- [[2]]
             [?b]
             (data ?a)
             (get-in ?a [:a :b] :> ?b))))

(deftest test-reorder-generator-vars
  (let [nums [["a" 1]]
        kv (<- [?l ?n]
               (nums ?l ?n))]
    (test?<- [1]
             [?n]
             (kv ?l ?n))

    (test?<- ["a"]
             [?l]
             (kv ?l ?n))))

(deftest test-multi-query-parallel
  (let [test-data [["ben" 35]
                   ["jerry" 41]]]
    (??- (<- [?name ?age]
             (test-data ?name ?age)
             (< ?age 40))
         (<- [?name ?age]
             (test-data ?name ?age)
             (< ?age 50)))
    => (produces [["ben" 35]
                  [["ben" 35] ["jerry" 41]]])))

(deftest test-implicit-equality-constraints
  (let [num-pair [
                  [1 2]
                  [0 0]
                  [1 1]
                  [4 4]
                  [8 3]
                  [4 0]
                  ]]
    (fact
     (<- [?n]
         (num-pair :> ?n ?n))
     => (produces [[0] [1] [4]]))))
