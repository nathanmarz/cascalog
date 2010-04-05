(ns cascalog.predicate
  (:use [clojure.contrib.seq-utils :only [partition-by]])
  (:use [cascalog vars util])
  (:require [cascalog [workflow :as w]])
  (:import [cascading.tap Tap])
  (:import [cascading.tuple Fields]))

;;  type is :operation, for map, mapcat, filter
(defstruct operation-predicate :type :assembly :infields :outfields)

;; type is one of aggregator or buffer
;; pre-group assembly is for prep stuff like variable sub, null checking, and fast aggregators (counting/summing)
(defstruct aggregator-predicate :type :pregroup-assembly :postgroup-assembly :infields :outfields)

;;  type is :generator
;; automatically generates source pipes and attaches to sources
(defstruct generator-predicate :type :sourcemap :pipe :outfields)


(defstruct predicate-variables :in :out)

(defn parse-variables
  "parses variables of the form ['?a' '?b' :> '!!c']
   If there is no :>, defaults to in-or-out-default (:in or :out)"
  [vars in-or-out-default]
  (let [split (partition-by keyword? vars)
        amt   (count split)
        var-base (struct predicate-variables [] [])]
        (cond (= amt 1) (merge var-base {in-or-out-default (first split)})
              (= amt 3) (struct predicate-variables (first split) (nth split 2))
              true      (throw (IllegalArgumentException. (str "Bad variables inputted " vars))))
        ))

;; hacky, but best way to do it given restrictions of needing a var for regular functions and needing 
;; to seemlessly integrate with normal workflows
(defn- predicate-dispatcher [op & rest]
  (cond (instance? Tap op) ::tap
        (map? op) ::generator
        (not (nil? (w/get-op-metadata op))) (:type (w/get-op-metadata op))
        (fn? op) ::vanilla-function
        true (throw (IllegalArgumentException. "Bad predicate"))
        ))

(defmulti predicate-default-var predicate-dispatcher)

(defmethod predicate-default-var ::tap [& args] :out)
(defmethod predicate-default-var ::generator [& args] :out)
(defmethod predicate-default-var ::vanilla-function [& args] :out)
(defmethod predicate-default-var :map [& args] :out)
(defmethod predicate-default-var :mapcat [& args] :out)
(defmethod predicate-default-var :aggregate [& args] :out)
(defmethod predicate-default-var :buffer [& args] :out)
(defmethod predicate-default-var :filter [& args] :in)

(defmulti build-predicate-specific predicate-dispatcher)

;; TODO: should have a (generator :only ?a ?b) syntax for generators (only select those fields, filter the rest)
(defmethod build-predicate-specific ::tap [tap _ infields outfields]
  (let
    [assembly (w/identity Fields/ALL :fn> outfields :> Fields/RESULTS)]
    (when-not (empty? infields) (throw (IllegalArgumentException. "Cannot use :> in a taps vars declaration")))
    (struct generator-predicate :generator [tap] assembly outfields)
  ))

(defmethod build-predicate-specific ::generator [gen _ infields outfields]
  (let [gen-assembly (:assembly gen)
        assem (w/compose-straight-assemblies gen-assembly (w/identity Fields/ALL :fn> outfields :> Fields/RESULTS))]
  (struct generator-predicate :generator (:sources gen) assem outfields)))

(defmethod build-predicate-specific ::vanilla-function [_ opvar infields outfields]
  (when (nil? opvar) (throw (RuntimeException. "Functions must have vars associated with them")))
  (let
    [[func-fields out-selector] (if (not-empty outfields) [outfields Fields/ALL] [nil nil])
     assembly (w/filter opvar infields :fn> func-fields :> out-selector)]
    (struct operation-predicate :operation assembly infields outfields)))

(defn- standard-build-predicate [op _ infields outfields]
    (struct operation-predicate :operation (op infields :fn> outfields :> Fields/ALL) infields outfields))

(defmethod build-predicate-specific :map [& args]
  (apply standard-build-predicate args))

(defmethod build-predicate-specific :mapcat [& args]
  (apply standard-build-predicate args))

(defmethod build-predicate-specific :filter [op _ infields outfields]
  (let [[func-fields out-selector] (if (not-empty outfields) [outfields Fields/ALL] [nil nil])
     assembly (op infields :fn> func-fields :> out-selector)]
    (struct operation-predicate :operation assembly infields outfields)))

(defmethod build-predicate-specific :aggregate [& args]
  (apply standard-build-predicate args))

(defmethod build-predicate-specific :buffer [& args]
  (apply standard-build-predicate args))

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

(defn build-predicate
  "Build a predicate. Calls down to build-predicate-specific for predicate-specific building 
  and adds constant substitution and null checking of ? vars."
  [op opvar variables-args]
  (let [{orig-infields :in outfields :out} (parse-variables variables-args (predicate-default-var op))
       outfields                      (replace-ignored-vars outfields)
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
        (if (= :generator (:type predicate))
          (merge predicate {:pipe (outassembly (:pipe predicate))
                            :outfields new-outfields})
          (merge predicate {:assembly (apply w/compose-straight-assemblies (filter (complement nil?)
                                      [in-insertion-assembly (:assembly predicate) outassembly]
                                      ))
                            :outfields new-outfields
                            :infields (filter cascalog-var? orig-infields)}))))