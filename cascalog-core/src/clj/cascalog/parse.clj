(ns cascalog.parse
  (:require [clojure.set :refer (difference)]
            [jackknife.core :refer (throw-illegal throw-runtime)]
            [jackknife.seq :as s]
            [cascalog.vars :as v]
            [cascalog.predicate :as p]
            [cascalog.options :as opts]
            [cascalog.fluent.types :refer (generator?)]
            [cascalog.fluent.operations :as ops])
  (:import [jcascalog Predicate]
           [clojure.lang IPersistentVector]))

;; This thing handles the logic that the output of an operation, if
;; marked as a constant, should be a filter.
;;
;; This gets called twice, since we might have some new output
;; variables after converting generator as set logic.

(defn split-outvar-constants
  [{:keys [op invars outvars] :as m}]
  (let [[new-outvars newpreds] (reduce
                                (fn [[outvars preds] v]
                                  (if (v/cascalog-var? v)
                                    [(conj outvars v) preds]
                                    (let [newvar (v/gen-nullable-var)]
                                      [(conj outvars newvar)
                                       (conj preds {:op (p/predicate
                                                         p/outconstant-equal)
                                                    :invars [v newvar]
                                                    :outvars []})])))
                                [[] []]
                                outvars)]
    (-> (assoc m :outvars new-outvars)
        (cons newpreds))))

;; Handles the output for generator as set.

(defn- rewrite-predicate
  [{:keys [op invars outvars] :as predicate}]
  (if-not (and (generator? op) (seq invars))
    predicate
    (if (= 1 (count outvars))
      {:op (p/predicate p/generator-filter
                        op
                        (first outvars))
       :invars []
       :outvars invars}
      (throw-illegal (str "Generator filter can only have one outvar -> " outvars)))))

;; ## Variable Parsing
;;
;; The following functions deal with parsing of logic variables out of
;; predicates.

;; TODO: validation on the arg-m. We shouldn't ever have the sugar arg
;; and the non-sugar arg. Move the examples out to tests.

(defn desugar-selectors
  "Accepts a map of cascalog input or output symbol (:< or :>, for
  example) to var sequence, a <sugary input or output selector> and a
  <full vector input or output selector> and either destructures the
  non-sugary input or moves the sugary input into its proper
  place. For example:

 (desugar-selectors {:>> ([\"?a\"])} :> :>>)
 ;=> {:>> [\"?a\"]}

 (desugar-selectors {:> [\"?a\"] :<< [[\"?b\"]]} :> :>> :< :<<)
 ;=>  {:>> [\"?a\"], :<< [\"?b\"]}"
  [arg-m & sugar-full-pairs]
  (letfn [(desugar [m [sugar-k full-k]]
            (if-not (some m #{sugar-k full-k})
              m
              (-> m
                  (dissoc sugar-k)
                  (assoc full-k
                    (or (first (m full-k))
                        (m sugar-k))))))]
    (reduce desugar arg-m
            (partition 2 sugar-full-pairs))))

(defn expand-positional-selector
  "Accepts a map of cascalog selector to var sequence and, if the map
  contains an entry for Cascalog's positional selector, expands out
  the proper number of logic vars and replaces each entry specified
  within the positional map. This function returns the updated map."
  [arg-m]
  (if-let [[amt selector-map] (arg-m :#>)]
    (let [expanded-vars (reduce (fn [v [pos var]]
                                  (assoc v pos var))
                                (vec (v/gen-nullable-vars amt))
                                selector-map)]
      (-> arg-m
          (dissoc :#>)
          (assoc :>> expanded-vars)))
    arg-m))

;; TODO: Remove the pre assertion on jackknife.seq/unweave, move this
;; over and republish.

(defn unweave
  "[1 2 3 4 5 6] -> [[1 3 5] [2 4 6]]"
  [coll]
  [(take-nth 2 coll) (take-nth 2 (rest coll))])

(defn parse-predicate-vars
  "Accepts a normalized cascalog predicate vector of normalized
  vars (without the ops) and returns a map of cascalog reserved
  keyword to the logic variables immediately following."
  [vars]
  {:pre [(keyword? (first vars))]}
  (let [[keywords vars] (->> vars
                             (partition-by v/cascalog-keyword?)
                             (unweave))]
    (zipmap (map first keywords)
            vars)))

;; TODO: Note that this is the spot where we'd go ahead and add new
;; selectors to Cascalog. For example, say we wanted the ability to
;; pour the results of a query into a vector directly; :>> ?a. This is
;; the place.

(defn parse-variables
  "parses variables of the form ['?a' '?b' :> '!!c'] and returns a map
   of input variables, output variables, If there is no :>, defaults
   to selector-default."
  [vars selector-default]
  {:pre (contains? #{:< :>} selector-default)}
  ;; First, if we start with a cascalog keyword, don't worry about
  ;; it. If we don't, we probably need to append something; if we're
  ;; dealing with a predicate macro, don't do anything, otherwise just
  ;; tag the supplied selector-default onto the front.
  (let [vars (if (v/cascalog-keyword? (first vars))
               vars
               (-> (if (some v/cascalog-keyword? vars)
                     :<
                     selector-default)
                   (cons vars)))]
    (-> vars
        (parse-predicate-vars)
        (desugar-selectors :> :>>
                           :< :<<)
        (expand-positional-selector)
        (select-keys #{:<< :>>}))))

(defn parse-predicate [[op vars]]
  (let [{invars :<< outvars :>>}
        (parse-variables vars (p/default-var op))]
    {:op op
     :invars invars
     :outvars outvars}))

;; ## Gen-As-Set Validation

(defn- unzip-generators
  "Returns a vector containing two sequences; the subset of the
  supplied sequence of parsed-predicates identified as generators, and
  the rest."
  [parsed-preds]
  (s/separate (comp generator? :op) parsed-preds))

(defn gen-as-set?
  "Returns true if the supplied parsed predicate is a generator meant
  to be used as a set, false otherwise."
  [parsed-pred]
  (and (generator? (:op parsed-pred))
       (not-empty  (:invars parsed-pred))))

(defn- gen-as-set-ungrounding-vars
  "Returns a sequence of ungrounding vars present in the
  generators-as-sets contained within the supplied sequence of parsed
  predicates (maps with keys :op, :invars, :outvars)."
  [parsed-preds]
  (mapcat (comp (partial filter v/unground-var?)
                #(mapcat % [:invars :outvars]))
          (filter gen-as-set? parsed-preds)))

(defn- parse-ungrounding-outvars
  "For the supplied sequence of parsed cascalog predicates with keys
  `[op invars outvars]`, returns a vector of two entries: a sequence
  of all output ungrounding vars that appear within generator
  predicates, and a sequence of all ungrounding vars that appear
  within non-generator predicates."
  [parsed-preds]
  (map (comp (partial mapcat #(filter v/unground-var? %))
             (partial map :outvars))
       (unzip-generators parsed-preds)))

(defn- pred-clean!
  "Performs various validations on the supplied set of parsed
  predicates. If all validations pass, returns the sequence
  unchanged."
  [parsed-preds]
  (let [gen-as-set-vars (gen-as-set-ungrounding-vars parsed-preds)
        [gen-outvars pred-outvars] (parse-ungrounding-outvars parsed-preds)
        extra-vars  (vec (difference (set pred-outvars)
                                     (set gen-outvars)))
        dups (s/duplicates gen-outvars)]
    (cond
     (not-empty gen-as-set-vars)
     (throw-illegal (str "Can't have unground vars in generators-as-sets."
                         (vec gen-as-set-vars)
                         " violate(s) the rules.\n\n" (pr-str parsed-preds)))

     (not-empty extra-vars)
     (throw-illegal (str "Ungrounding vars must originate within a generator. "
                         extra-vars
                         " violate(s) the rules."))

     (not-empty dups)
     (throw-illegal (str "Each ungrounding var can only appear once per query."
                         "The following are duplicated: "
                         dups))
     :else parsed-preds)))

(defprotocol IPredicate
  (to-predicate [_]
    "Returns an instance of RawPredicate.

   [* [\"?x\" \"?x\" :> \"?y\"]]"))

;; Raw Predicate type.
(defrecord RawPredicate [op input output options]
  IPredicate
  (to-predicate [this] this))

(extend-protocol IPredicate
  Predicate
  (to-predicate [pred]
    (.toRawCascalogPredicate pred))

  IPersistentVector
  (to-predicate [pred]
    (let [[op & vars] pred
          {invars :<< outvars :>>}
          (-> (v/sanitize vars)
              (parse-variables (p/default-var op)))]
      (->RawPredicate op invars outvars {}))))

;; Output of the subquery, the predicates it contains and the options
;; in the subquery.
;;
;; TODO: Have this thing extend IGenerator.
(defrecord RawSubquery [fields predicates options])

(comment
  "Make some functions that pull these out."
  (defn generators [sq])
  (defn operations [sq])
  (defn aggregators [sq])

  ;; It'd be nice to use trammel here for contracts.

  (defn validate
    "Returns the subquery if valid, fails otherwise."
    [sq]
    (let [aggs (aggregators sq)]
      (if (and (> (count aggs) 1)
               (some buffer? aggs))
        (throw-illegal "Cannot use both aggregators and buffers in same grouping")
        sq))))

(defn parse-predicates [raw-predicates]
  (->> raw-predicates
       (map parse-predicate)
       (pred-clean!)
       (mapcat split-outvar-constants)
       (map rewrite-predicate)
       (mapcat split-outvar-constants)))

;; Damn, looks like cascalog.vars has some pretty good stuff for this!

(defn parse-subquery
  "Parses predicates and output fields and returns a proper subquery."
  [output-fields raw-predicates]
  (let [[raw-options raw-predicates] (s/separate (comp keyword? first)
                                                 raw-predicates)
        option-map     (opts/generate-option-map raw-options)
        raw-predicates (parse-predicates raw-predicates)]
    (->RawSubquery output-fields
                   (map to-predicate raw-predicates)
                   option-map)))
