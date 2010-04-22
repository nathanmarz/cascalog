 ;    Copyright 2010 Nathan Marz
 ; 
 ;    This program is free software: you can redistribute it and/or modify
 ;    it under the terms of the GNU General Public License as published by
 ;    the Free Software Foundation, either version 3 of the License, or
 ;    (at your option) any later version.
 ; 
 ;    This program is distributed in the hope that it will be useful,
 ;    but WITHOUT ANY WARRANTY; without even the implied warranty of
 ;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 ;    GNU General Public License for more details.
 ; 
 ;    You should have received a copy of the GNU General Public License
 ;    along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns cascalog.predicate
  (:use [clojure.contrib.seq-utils :only [partition-by]])
  (:use [cascalog vars util])
  (:require [cascalog [workflow :as w]])
  (:import [cascading.tap Tap])
  (:import [cascading.tuple Fields])
  (:import [cascalog ClojureParallelAggregator]))

;; doing it this way b/c pain to put metadata directly on a function
;; assembly-maker is a function that takes in infields & outfields and returns
;; [preassembly postassembly]
(defstruct parallel-aggregator :init-var :combine-var :args)

(defmacro defparallelagg [name & body]
  `(def ~name (struct-map cascalog.predicate/parallel-aggregator ~@body)))

;; ids are so they can be used in sets safely
(defmacro defpredicate [name & attrs]
  `(defstruct ~name :type :id ~@attrs))

(defmacro predicate [aname & attrs]
  `(struct ~aname ~(keyword (name aname)) (uuid) ~@attrs))

;; for map, mapcat, and filter
(defpredicate operation :assembly :infields :outfields)

;; return a :post-assembly, a :parallel-agg, and a :serial-agg-assembly
(defpredicate aggregator :composable :parallel-agg :pregroup-assembly :serial-agg-assembly :post-assembly :infields :outfields)
;; automatically generates source pipes and attaches to sources
(defpredicate generator :sourcemap :pipe :outfields)


;; TODO: change this to use fast first buffer
(def distinct-aggregator (predicate aggregator false nil identity (w/first) identity [] []))

(defstruct predicate-variables :in :out)

(defn parse-variables
  "parses variables of the form ['?a' '?b' :> '!!c']
   If there is no :>, defaults to in-or-out-default (:in or :out)"
  [vars in-or-out-default]
  (let [split (partition-by (partial = :>) vars)
        amt   (count split)
        var-base (struct predicate-variables [] [])]
        (cond (= amt 1) (merge var-base {in-or-out-default (first split)})
              (= amt 3) (struct predicate-variables (first split) (nth split 2))
              true      (throw (IllegalArgumentException. (str "Bad variables inputted " vars))))
        ))

;; hacky, but best way to do it given restrictions of needing a var for regular functions, needing 
;; to seemlessly integrate with normal workflows, and lack of function metadata in clojure (until 1.2 anyway)
;; uses hacky function metadata so that operations can be passed in as arguments when constructing cascalog
;; rules
(defn- predicate-dispatcher [op & rest]
  (cond (instance? Tap op) ::tap
        (map? op) (if (= :generator (:type op)) ::generator ::parallel-aggregator)
        (w/get-op-metadata op) (:type (w/get-op-metadata op))
        (fn? op) ::vanilla-function
        true (throw (IllegalArgumentException. "Bad predicate"))
        ))

(defmulti predicate-default-var predicate-dispatcher)

(defmethod predicate-default-var ::tap [& args] :out)
(defmethod predicate-default-var ::generator [& args] :out)
(defmethod predicate-default-var ::parallel-aggregator [& args] :out)
(defmethod predicate-default-var ::vanilla-function [& args] :in)
(defmethod predicate-default-var :map [& args] :out)  ; doesn't matter
(defmethod predicate-default-var :mapcat [& args] :out)  ; doesn't matter
(defmethod predicate-default-var :aggregate [& args] :out)
(defmethod predicate-default-var :buffer [& args] :out)
(defmethod predicate-default-var :filter [& args] :in)

(defmulti build-predicate-specific predicate-dispatcher)

;; TODO: should have a (generator :only ?a ?b) syntax for generators (only select those fields, filter the rest)
(defmethod build-predicate-specific ::tap [tap _ infields outfields]
  (let
    [pname (uuid)
     pipe (w/assemble (w/pipe pname) (w/identity Fields/ALL :fn> outfields :> Fields/RESULTS))]
    (when-not (empty? infields) (throw (IllegalArgumentException. "Cannot use :> in a taps vars declaration")))
    (predicate generator {pname tap} pipe outfields)
  ))

(defmethod build-predicate-specific ::generator [gen _ infields outfields]
  (let [gen-pipe (w/assemble (:pipe gen) (w/pipe-rename (uuid)) (w/identity Fields/ALL :fn> outfields :> Fields/RESULTS))]
  (predicate generator (:sourcemap gen) gen-pipe outfields)))

(defmethod build-predicate-specific ::vanilla-function [_ opvar infields outfields]
  (when (nil? opvar) (throw (RuntimeException. "Functions must have vars associated with them")))
  (let
    [[func-fields out-selector] (if (not-empty outfields) [outfields Fields/ALL] [nil nil])
     assembly (w/filter opvar infields :fn> func-fields :> out-selector)]
    (predicate operation assembly infields outfields)))

(defn- simpleop-build-predicate [op _ infields outfields]
    (predicate operation (op infields :fn> outfields :> Fields/ALL) infields outfields))

(defmethod build-predicate-specific :map [& args]
  (apply simpleop-build-predicate args))

(defmethod build-predicate-specific :mapcat [& args]
  (apply simpleop-build-predicate args))

(defmethod build-predicate-specific :filter [op _ infields outfields]
  (let [[func-fields out-selector] (if (not-empty outfields) [outfields Fields/ALL] [nil nil])
     assembly (op infields :fn> func-fields :> out-selector)]
    (predicate operation assembly infields outfields)))

(defmethod build-predicate-specific ::parallel-aggregator [pagg _ infields outfields]
  (when (or (not= (count infields) (:args pagg)) (not= 1 (count outfields)))
    (throw (IllegalArgumentException. (str "Invalid # input fields to aggregator " pagg))))
  (let [init-spec (w/fn-spec (:init-var pagg))
        combine-spec (w/fn-spec (:combine-var pagg))
        cascading-agg (ClojureParallelAggregator. (first outfields) init-spec combine-spec (:args pagg))
        serial-assem (if (empty? infields)
                        (w/raw-every cascading-agg Fields/ALL)
                        (w/raw-every (w/fields infields)
                                  cascading-agg
                                  Fields/ALL))]
    (predicate aggregator true (assoc pagg :outfield (first outfields)) identity serial-assem identity infields outfields)))


(defn- simpleagg-build-predicate [composable op _ infields outfields]
  (predicate aggregator composable nil identity (op infields :fn> outfields :> Fields/ALL) identity infields outfields))

(defmethod build-predicate-specific :aggregate [& args]
  (apply simpleagg-build-predicate true args))

(defmethod build-predicate-specific :buffer [& args]
  (apply simpleagg-build-predicate false args))

(defn- variable-substitution
  "Returns [newvars {map of newvars to values to substitute}]"
  [vars]
  (substitute-if (complement cascalog-var?) (fn [_] (gen-nullable-var)) vars))

(defn- output-substitution
  "Returns [{newvars map to constant values} {old vars to new vars that should be equal}]"
  [sub-map]
  (reduce (fn [[newvars equalities] [oldvar value]]
    (let [v (gen-nullable-var)]
      [(assoc newvars v value) (assoc equalities oldvar v)]))
    [{} {}] (seq sub-map)))

(w/deffilterop non-null? [& objs]
  (every? (complement nil?) objs))

(defn- mk-insertion-assembly [subs]
  (when (not-empty subs)
    (apply w/insert (transpose (seq subs)))))

(defn- replace-ignored-vars [vars]
  (map #(if (= "_" %) (gen-nullable-var) %) vars))

(defn- mk-null-check [fields]
  (let [non-null-fields (filter non-nullable-var? fields)]
    (when (not-empty non-null-fields)
      (non-null? non-null-fields))))

(defmulti enhance-predicate (fn [pred & rest] (:type pred)))

(defn- identity-if-nil [a]
  (if a a identity))

(defmethod enhance-predicate :operation [pred infields inassem outfields outassem]
  (let [inassem (identity-if-nil inassem)
        outassem (identity-if-nil outassem)]
    (merge pred {:assembly (w/compose-straight-assemblies inassem (:assembly pred) outassem)
                 :outfields outfields
                 :infields infields})))

(defmethod enhance-predicate :aggregator [pred infields inassem outfields outassem]
  (let [inassem (identity-if-nil inassem)
        outassem (identity-if-nil outassem)]
    (merge pred {:pregroup-assembly (w/compose-straight-assemblies inassem (:pregroup-assembly pred))
                 :post-assembly (w/compose-straight-assemblies (:post-assembly pred) outassem)
                 :outfields outfields
                 :infields infields})))

(defmethod enhance-predicate :generator [pred infields inassem outfields outassem]
  (when inassem
    (throw (RuntimeException. "Something went wrong in planner - generator received an input modifier")))
  (merge pred {:pipe (outassem (:pipe pred))
               :outfields outfields}))

(defn build-predicate
  "Build a predicate. Calls down to build-predicate-specific for predicate-specific building 
  and adds constant substitution and null checking of ? vars."
  [op opvar orig-infields outfields]
  (let [outfields                      (replace-ignored-vars outfields)
        [infields infield-subs]        (variable-substitution orig-infields)
        [outfields outfield-subs]      (variable-substitution outfields)
        predicate                      (build-predicate-specific op opvar infields outfields)
        [newsubs equalities]           (output-substitution outfield-subs)
        new-outfields                  (concat outfields (keys newsubs) (keys infield-subs))
        in-insertion-assembly          (mk-insertion-assembly infield-subs)
        out-insertion-assembly         (mk-insertion-assembly newsubs)
        null-check-out                 (mk-null-check new-outfields)
        equality-assemblies            (map w/equal equalities)
        outassembly                    (apply w/compose-straight-assemblies
                                        (filter (complement nil?)
                                          (concat [out-insertion-assembly
                                                  null-check-out]
                                        equality-assemblies)))]
        (enhance-predicate predicate
                           (filter cascalog-var? orig-infields)
                           in-insertion-assembly
                           new-outfields
                           outassembly)))