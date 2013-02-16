(ns cascalog.elephantdb.keyval-test
  (:use cascalog.elephantdb.keyval
        cascalog.elephantdb.core
        cascalog.api
        clojure.test
        [midje sweet cascalog])
  (:require [hadoop-util.test :as test]
            [cascalog.ops :as c]
            [cascalog.elephantdb.example :as e]
            [elephantdb.common.config :as config])
  (:import [elephantdb.persistence JavaBerkDB]
           [elephantdb.partition HashModScheme]))

(defn mk-spec [num-shards]
  {:num-shards  num-shards
   :coordinator (JavaBerkDB.)
   :shard-scheme (HashModScheme.)})

(defn vec-merge
  [a b]
  (vec (merge (into {} a)
              (into {} b))))

(defmacro with-kv-tap
  "Accepts a binding vector with the tap-symbol (for binding, as with
  `let`), the shard-count and optional keyword arguments to be passed
  on to `kv-opts` above.

  `with-kv-tap` accepts a `:log-level` optional keyword argument that
  can be used to tune the output of all jobs run within the
  form. Valid log level values or `:fatal`, `:warn`, `:info`, `:debug`
  and `:off`.

  To change the configuration map used for the test, supply a map
  using the `:conf` keyword argument."
  [[sym shard-count & opts] & body]
  `(test/with-fs-tmp [fs# tmp#]
     (let [~sym (keyval-tap tmp#
                            :spec (mk-spec ~shard-count)
                            ~@opts)]
       ~@body)))

(tabular
 (fact
   "Tuples sunk into an ElephantDB tap and read back out should
    match. (A map acts as a sequence of 2-tuples, perfect for
    ElephantDB key-val tests.)"
   (with-kv-tap [e-tap 4]

     "Execute tuples into the keyval-tap"
     (?- e-tap ?tuples)

     "Do we get the same tuples back out?"
     e-tap => (produces ?tuples)))
 ?tuples
 [[1 2] [3 4]]
 [["key" "val"] ["ham" "burger"]])

(facts
  "When incremental is set to false, ElephantDB should generate a new
   domain completely independent of the old domain."
  (with-kv-tap [sink 4 :incremental false]
    (let [data (vec {0 "zero"
                     1 "one"
                     2 "two"
                     3 "three"
                     4 "four"
                     5 "five"
                     6 "six"
                     7 "seven"
                     8 "eight"})
          data2 (vec {0 "zero!!"
                      10 "ten"})]
      (fact "Populating the sink with `data` produces `data`."
        (?- sink data)
        sink => (produces data))

      (fact "Sinking data2 on top of data should knock out all old
      values, leaving only data2."
        (?- sink data2)
        sink => (produces data2)))))

(facts
  "Incremental defaults to `true`, bringing an updater into
  play. For a key-value store, the default behavior on an incremental
  update is for new kv-pairs to knock out existing kv-pairs."
  (with-kv-tap [sink 2]
    (let [data  (vec {0 "zero"
                      1 "one"
                      2 "two"})
          data2 (vec {0 "ZERO!"
                      3 "THREE!"})]
      (fact "Populating the sink with `data` produces `data`."
        (?- sink data)
        sink => (produces data))

      (fact "Sinking `data2` on top of `data` produces the same set
        of tuples as merging two clojure maps"
        (?- sink data2)
        sink => (produces (vec-merge data data2))))))

(tabular
 (fact
   "Explicitly set an indexer to customize the incremental update
  behavior. This test checks that the StringAppendIndexer merges by
  appending the new value onto the old value instead of knocking out
  the old value completely."
   (with-kv-tap [sink 2 :indexer ?indexer]
     (let [data   (vec {0 "zero"
                        1 "one"
                        2 "two"})
           data2  (vec {0 "ZERO!"
                        2 "TWO!"
                        3 "THREE!"})
           merged (vec {0 "zeroZERO!"
                        1 "one"
                        2 "twoTWO!"
                        3 "THREE!"})]
       (fact "Populating the sink with `data` produces `data`."
         (?- sink data)
         sink => (produces data))
       
       (fact "Sinking `data2` on top of `data` causes clashing values
        to merge with a string-append."
         (?- sink data2)
         sink => (produces merged)

         "Note that `merged` can be created with clojure's `merge-with`."
         (merge-with str
                     (into {} data)
                     (into {} data2)) => (into {} merged)))))
 ?indexer
 (mk-indexer #'e/str-indexer)
 #'e/str-kv-indexer)

(defn spec-has [m]
  (chatty-checker
   [[fs path]]
   ((contains m) (config/read-domain-spec fs path))))

(fact "Resharding shouldn't affect the data at all."
  (test/with-fs-tmp [fs base-path tmp-a tmp-b]
    (let [tap   (keyval-tap base-path :spec (mk-spec 3))
          pairs (vec {0 1, 1 2, 2 3, 3 0, 4 0, 5 1})]

      "Send the pairs into the initial tap."
      (?- tap pairs)

      "The spec should have the proper number of shards,"
      [fs base-path] => (spec-has {:num-shards 3})

      "And the original path should produce pairs."
      (keyval-tap base-path) => (produces pairs)

      "Reshard the domain into a single shard;"
      (reshard! base-path tmp-a 1)

      "the spec at tmp-a should now have only 1 shard,"
      [fs tmp-a] => (spec-has {:num-shards 1})

      "while producing the same pairs."
      (keyval-tap tmp-a) => (produces pairs)

      "One more iteration should work just fine."
      (reshard! tmp-a tmp-b 5)
      [fs tmp-b] => (spec-has {:num-shards 5})
      (keyval-tap tmp-b) => (produces pairs))))
