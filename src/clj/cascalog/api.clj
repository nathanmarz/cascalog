(ns cascalog.api
  (:use [cascalog vars util graph])
  (:require cascalog.rules)
  (:require [cascalog [workflow :as w]])
  (:import [cascading.flow Flow FlowConnector])
  (:import  [cascading.pipe Pipe]))

;; TODO: add builtins here like fast count (what to call it?), !count, sum, min, max, etc.

; (p/defcomplexagg count [infields outfields]
;   )
; 
; (p/defcomplexagg sum [infields outfields]
;   )

(defmacro <-
  "Constructs a rule from a list of predicates"
  [outvars & predicates]
  (let [predicate-builders (vec (map cascalog.rules/mk-raw-predicate predicates))
        outvars-str (vars2str outvars)]
        `(cascalog.rules/build-rule ~outvars-str ~predicate-builders)))

;; TODO: add ability to specify sorting of output, specify whether or not to distinct on map-only
(defn ?-
  "Builds and executes a flow based on the sinks binded to the rules. 
  Bindings are of form: sink rule"
  [& bindings]
  (when (odd? (count bindings)) (throw (IllegalArgumentException. "Need even number of args to ?-")))
  (let [sinks           (take-nth 2 bindings)
        gens            (take-nth 2 (rest bindings))
        sourcemap       (apply merge (map :sourcemap gens))
        tails           (map cascalog.rules/connect-to-sink gens sinks)
        sinkmap         (w/taps-map tails sinks)
        flow            (.connect (FlowConnector.) sourcemap sinkmap (into-array Pipe tails))]
        (.complete flow)))

(defmacro ?<- [output outvars & predicates]
  `(?- ~output (<- ~outvars ~@predicates)))