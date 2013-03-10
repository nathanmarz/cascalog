(ns cascalog.elephantdb.keyval-test
  (:use cascalog.elephantdb.keyval
        cascalog.elephantdb.core
        cascalog.elephantdb.conf
        cascalog.api
        clojure.test
        [midje sweet cascalog])
  (:require [hadoop-util.test :as test]
            [cascalog.ops :as c]
            [cascalog.elephantdb.example :as e])
  (:import [elephantdb.persistence JavaBerkDB]
           [elephantdb.partition HashModScheme]))

;; ## Byte Array Testing

(defn str->barr [str]
  (.getBytes str "UTF-8"))

(defn barr->str [^bytes bs]
  (String. bs))

(defn barr [& xs]
  (when xs
    (byte-array (map byte xs))))

(defn barr=
  ([x] true)
  ([^bytes x ^bytes y] (java.util.Arrays/equals x y))
  ([x y & more]
     (if (barr= x y)
       (if (next more)
         (recur y (first more) (next more))
         (barr= y (first more)))
       false)))

(defn count= [& colls]
  (apply = (map count colls)))

(defn barrs=
  [& barr-seqs]
  (and (apply count= barr-seqs)
       (every? true? (apply map barr= barr-seqs))))

(defn barrs-checker [expected-ds]
  (chatty-checker [actual-ds]
                  (barrs= (first expected-ds)
                          (first actual-ds))))

(def produces-barrs (wrap-checker barrs-checker))

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
     e-tap => (produces-barrs ?tuples)))
 ?tuples
 [[(.getBytes "key") (.getBytes "val")]])

(defn spec-has [m]
  (chatty-checker
   [[fs path]]
   ((contains m) (read-domain-spec fs path))))

(future-fact "Resharding shouldn't affect the data at all."
  (test/with-fs-tmp [fs base-path tmp-a tmp-b]
    (let [tap   (keyval-tap base-path :spec (mk-spec 3))
          pairs (vec {(barr 0) (barr 1), (barr 1) (barr 2), (barr 2) (barr 3), (barr 3) (barr 0), (barr 4) (barr 0), (barr 5) (barr 1)})]

      "Send the pairs into the initial tap."
      (?- tap pairs)

      "The spec should have the proper number of shards,"
      [fs base-path] => (spec-has {:num-shards 3})

      "And the original path should produce pairs."
      (keyval-tap base-path) => (produces-barrs pairs)

      "Reshard the domain into a single shard;"
      (reshard! base-path tmp-a 1)

      "the spec at tmp-a should now have only 1 shard,"
      [fs tmp-a] => (spec-has {:num-shards 1})

      "while producing the same pairs."
      (keyval-tap tmp-a) => (produces-barrs pairs)

      "One more iteration should work just fine."
      (reshard! tmp-a tmp-b 5)
      [fs tmp-b] => (spec-has {:num-shards 5})
      (keyval-tap tmp-b) => (produces-barrs pairs))))
