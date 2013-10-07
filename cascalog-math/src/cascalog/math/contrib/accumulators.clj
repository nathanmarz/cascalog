;; Accumulators

;; by Konrad Hinsen
;; last updated May 19, 2009

;; This module defines various accumulators (list, vector, map,
;; sum, product, counter, and combinations thereof) with a common
;; interface defined by the multimethods add and combine.
;; For each accumulator type, its empty value is defined in this module.
;; Applications typically use this as a starting value and add data
;; using the add multimethod.

;; Copyright (c) Konrad Hinsen, 2009. All rights reserved.  The use
;; and distribution terms for this software are covered by the Eclipse
;; Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this
;; distribution.  By using this software in any fashion, you are
;; agreeing to be bound by the terms of this license.  You must not
;; remove this notice, or any other, from this software.

(ns
  ^{:author "Konrad Hinsen"
     :doc "A generic accumulator interface and implementations of various
           accumulators."}
  cascalog.math.contrib.accumulators
  (:refer-clojure :exclude (deftype))
  (:use [cascalog.math.contrib.types :only (deftype)])
  (:use [cascalog.math.contrib.def :only (defvar)]))

(defmulti add
  "Add item to the accumulator acc. The exact meaning of adding an
   an item depends on the type of the accumulator."
   {:arglists '([acc item])}
  (fn [acc item] (type acc)))

(defn add-items
  "Add all elements of a collection coll to the accumulator acc."
  [acc items]
  (reduce add acc items))

(defmulti combine
  "Combine the values of the accumulators acc1 and acc2 into a
   single accumulator of the same type."
  {:arglists '([& accs])}
  (fn [& accs] (type (first accs))))

;
; Mean and variance accumulator
;
(deftype ::mean-variance mean-variance)

(derive ::mean-variance ::accumulator)

(defvar empty-mean-variance (mean-variance {:n 0 :mean 0 :variance 0})
  "An empty mean-variance accumulator, combining sample mean and
   sample variance. Only numbers can be added.")

(defmethod combine ::mean-variance
  ([mv]
   mv)

  ([mv1 mv2]
   (let [{n1 :n mean1 :mean var1 :variance} mv1
	 {n2 :n mean2 :mean var2 :variance} mv2
	 n (+ n1 n2)
	 mean (/ (+ (* n1 mean1) (* n2 mean2)) n)
	 sq #(* % %)
	 c    (+ (* n1 (sq (- mean mean1))) (* n2 (sq (- mean mean2))))
	 var  (if (< n 2)
		0
		(/ (+ c (* (dec n1) var1) (* (dec n2) var2)) (dec n)))]
     (mean-variance {:n n :mean mean :variance var})))
   
  ([mv1 mv2 & mvs]
   (reduce combine (combine mv1 mv2) mvs)))

(defmethod add ::mean-variance
  [mv x]
  (let [{n :n mean :mean var :variance} mv
	n1 (inc n)
	d (- x mean)
	new-mean (+ mean (/ d n1))
	new-var (if (zero? n) 0 (/ (+ (* (dec n) var) (* d (- x new-mean))) n))]
    (mean-variance {:n n1 :mean new-mean :variance new-var})))

