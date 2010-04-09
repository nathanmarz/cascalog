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

(def DEFAULT-OPTIONS
  {:distinct true})

(defmacro <-
  "Constructs a rule from a list of predicates"
  [& args]
  (let [[farg & rargs] args
        [options outvars predicates] (if (map? farg)
                                      [(merge DEFAULT-OPTIONS farg) (first rargs) (rest rargs)]
                                      [DEFAULT-OPTIONS farg rargs])
        predicate-builders (vec (map cascalog.rules/mk-raw-predicate predicates))
        outvars-str (vars2str outvars)]
        `(cascalog.rules/build-rule ~options ~outvars-str ~predicate-builders)))

;; TODO: add ability to specify sorting of output (should this be specified in query or in <- options?)
(defn ?-
  "Builds and executes a flow based on the sinks binded to the rules. 
  Bindings are of form: sink rule"
  [& bindings]
  (let [[sinks gens]    (unweave bindings)
        sourcemap       (apply merge (map :sourcemap gens))
        tails           (map cascalog.rules/connect-to-sink gens sinks)
        sinkmap         (w/taps-map tails sinks)
        flow            (.connect (FlowConnector.) sourcemap sinkmap (into-array Pipe tails))]
        (.complete flow)))

(defmacro ?<- [output & body]
  `(?- ~output (<- ~@body)))