(ns cascalog.logic.testing
  (:require [clojure.test :refer :all]
            [cascalog.api :refer :all]
            [jackknife.seq :refer (collectify multi-set)]
            [cascalog.logic.platform :as platform]))

(defn doublify
  "Takes a sequence of tuples and converts all numbers to doubles.
   For example:
    (doublify [[1 :a] [2 :b]])
    ;; [[1.0 :a] [2.0 :b]]"
  [tuples]
  (vec (for [t tuples]
         (into [] (map (fn [v] (if (number? v) (double v) v))
                       (collectify t))))))

(defn is-specs= [set1 set2]
  (every? true? (doall
                 (map (fn [input output]
                        (let [input  (multi-set (doublify input))
                              output (multi-set (doublify output))]
                          (is (= input output))))
                      set1 set2))))

(defn is-tuplesets= [set1 set2]
  (is-specs= [set1] [set2]))

(defprotocol ITestable
  (process?- [_ bindings]
    "Used in testing, returns the result from processing the bindings"))

(defn test?- [& bindings]
  (let [[specs out-tuples] (process?- platform/*platform* bindings)]
      (is-specs= specs out-tuples)))

(defmacro test?<- [& args]
  (let [[begin body] (if (keyword? (first args))
                       (split-at 2 args)
                       (split-at 1 args))]
    `(test?- ~@begin (<- ~@body))))

(defmacro thrown?<- [error & body]
  `(is (~'thrown? ~error (<- ~@body))))
