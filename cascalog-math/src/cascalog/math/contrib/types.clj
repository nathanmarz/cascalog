;; Data types

;; by Konrad Hinsen
;; last updated May 3, 2009

;; Copyright (c) Konrad Hinsen, 2009. All rights reserved.  The use
;; and distribution terms for this software are covered by the Eclipse
;; Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this
;; distribution.  By using this software in any fashion, you are
;; agreeing to be bound by the terms of this license.  You must not
;; remove this notice, or any other, from this software.

(ns
  ^{:author "Konrad Hinsen"
     :doc "General and algebraic data types"}
  cascalog.math.contrib.types
  (:refer-clojure :exclude (deftype))
  (:use [cascalog.math.contrib.def :only (name-with-attributes)]))

;
; Utility functions
;
(defn- qualified-symbol
  [s]
  (symbol (str *ns*) (str s)))

;
; Data type definition
;
(defmulti deconstruct type)

(defmacro deftype
  "Define a data type by a type tag (a namespace-qualified keyword)
   and a symbol naming the constructor function. Optionally, a
   constructor and a deconstructor function can be given as well,
   the defaults being clojure.core/identity and clojure.core/list.
   The full constructor associated with constructor-name calls the
   constructor function and attaches the type tag to its result
   as metadata. The deconstructor function must return the arguments
   to be passed to the constructor in order to create an equivalent
   object. It is used for printing and matching."
  {:arglists
  '([type-tag constructor-name docstring? attr-map?]
    [type-tag constructor-name docstring? attr-map? constructor]
    [type-tag constructor-name docstring? attr-map? constructor deconstructor])}
  [type-tag constructor-name & options]
  (let [[constructor-name options]  (name-with-attributes
				      constructor-name options)
	[constructor deconstructor] options
	constructor   		    (if (nil? constructor)
		      		      'clojure.core/identity
		      		      constructor)
	deconstructor 		    (if (nil? deconstructor)
		      		     'clojure.core/list
		      		     deconstructor)]
    `(do
       (derive ~type-tag ::type)
       (let [meta-map# {:type ~type-tag
			::constructor
			    (quote ~(qualified-symbol constructor-name))}]
	 (def ~constructor-name
	      (comp (fn [~'x] (with-meta ~'x meta-map#)) ~constructor))
	 (defmethod deconstruct ~type-tag [~'x]
	   (~deconstructor (with-meta ~'x {})))))))

