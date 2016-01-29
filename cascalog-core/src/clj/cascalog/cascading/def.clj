(ns cascalog.cascading.def
  (:require [cascalog.logic.fn :as s]
            [cascalog.logic.def :as d]
            [jackknife.meta :refer (meta-update)]))

(defn prepared
  "Marks the supplied operation as needing to be prepared by
  Cascading. The supplied op should take two arguments and return
  another IFn for use by Cascading."
  [afn]
  (meta-update afn #(merge % {::prepared true})))

;; TODO: This runs into trouble if you want to return a map to use as
;; a function. Make an interface that we can reify to make a prepared
;; operation if we want a cleanup.

(defmacro prepfn
  "Defines a prepared operation. Pass in an argument vector of two
  items and return either a function or a Map with two
  keywords; :operate and :cleanup"
  [args & body] {:pre [(= 2 (count args))]}
  `(prepared (s/fn ~args ~@body)))

(defn prepared?
  "Returns true if the supplied operation needs to be supplied the
  FlowProcess and operation call by Cascading on instantiation, false
  otherwise."
  [op]
  (= true (-> op meta ::prepared)))

(d/defdefop defprepfn
  "Defines a prepared operation."
  `prepfn)
