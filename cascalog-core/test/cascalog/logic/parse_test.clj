(ns cascalog.logic.parse-test
  (:use midje.sweet
        clojure.test
        cascalog.logic.parse)
  (:require [cascalog.logic.predicate :as p]
            [cascalog.logic.options :as opts]
            [cascalog.logic.ops :as c]))

(fact
  "parse-variables expands selectors out into the proper unsugared
   forms."
  (parse-variables ['?a '?b :> 4] :>) => {:input ["?a" "?b"] :output [4]}

  "If a selector exists in the sequence, the default is ignored."
  (parse-variables ['?a '?b :> 4] :<) => {:input ["?a" "?b"] :output [4]}

  "In the absence, the default is used. See input:"
  (parse-variables ['?a '?b] :<) => {:input ["?a" "?b"] :output nil}

  "or output."
  (parse-variables ['?a '?b] :>) => {:input nil :output ["?a" "?b"]}

  "Only :< and :> are allowed as the default selector."
  (parse-variables ['?a '?b :> 4] :anything) => (throws AssertionError)
  )

(defn predicate->operation [options predicate]
  (->> predicate
       p/normalize
       first
       (p/build-predicate options)))

(defn mk-grouped-and-options [in-predicates]
  (let [raw-predicates (mapcat p/normalize in-predicates)
        [options predicates] (opts/extract-options raw-predicates)
        expanded (mapcat expand-outvars predicates)]
    {:grouped (->> expanded
                   (map (partial p/build-predicate options))
                   (group-by type)),
    :options options}))

(defn operation-equals? [op1 op2]
  (and
    (= (:name (meta (:op op1))) (:name (meta (:op op2))))
    (= (:input op1) (:input op2))
    (= (:output op1) (:output op2))))

(defn contains-op? [op ops] (some #(operation-equals? op %) ops))

(deftest test-prune-operations
  "Prune-operations will judiciously remove unnecessary operations"
  (let [gen-pred [[[1 2][3 4]] :> "?a" "?b"]
        minus-pred [#'- "?b" "?a" :> "?minus"]
        plus-pred [#'+ "?b" "?a" :> "?plus"]
        count-pred [#'c/count "?count"]
        even?-pred [#'even? "?plus"]
        inc-plus-pred [#'inc "?plus" :> "?inc-plus"]
        sort-pred [:sort "?plus"]]

    "Prune the plus-pred since it's out var it is not requested in out-fields"
    (let [{:keys [grouped options]} (mk-grouped-and-options [gen-pred minus-pred plus-pred])
          out-fields ["?minus"]
          minus-op (predicate->operation options minus-pred)
          pruned-operations (prune-operations out-fields grouped options)]
      (is (= 1 (count pruned-operations)))
      (is (contains-op? minus-op pruned-operations)))

    "Prune chained operations, i.e., remove both plus-pred and inc-plus-pred"
    (let [{:keys [grouped options]} (mk-grouped-and-options [gen-pred minus-pred plus-pred inc-plus-pred])
          out-fields ["?minus"]
          minus-op (predicate->operation options minus-pred)
          pruned-operations (prune-operations out-fields grouped options)]
      (is (= 1 (count pruned-operations)))
      (is (contains-op? minus-op pruned-operations)))

    "Do not prune if predicate outvar is used as input to another predicate"
    (let [{:keys [grouped options]} (mk-grouped-and-options [gen-pred minus-pred plus-pred even?-pred])
          out-fields ["?minus"]
          minus-op (predicate->operation options minus-pred)
          plus-op (predicate->operation options plus-pred)
          even?-op (predicate->operation options even?-pred)
          pruned-operations (prune-operations out-fields grouped options)]
      (is (= 3 (count pruned-operations)))
      (is (contains-op? minus-op pruned-operations))
      (is (contains-op? plus-op pruned-operations))
      (is (contains-op? even?-op pruned-operations)))

    "Do not prune filter predicates"
    (let [{:keys [grouped options]} (mk-grouped-and-options [gen-pred plus-pred even?-pred])
          out-fields ["?plus"]
          plus-op (predicate->operation options plus-pred)
          even?-op (predicate->operation options even?-pred)
          pruned-operations (prune-operations out-fields grouped options)]
      (is (= 2 (count pruned-operations)))
      (is (contains-op? plus-op pruned-operations))
      (is (contains-op? even?-op pruned-operations)))

    "Do not prune since a no-input predicate (count-pred) exists"
    (let [{:keys [grouped options]} (mk-grouped-and-options [gen-pred minus-pred plus-pred count-pred])
          out-fields ["?minus" "?count"]
          minus-op (predicate->operation options minus-pred)
          plus-op (predicate->operation options plus-pred)
          count-op (predicate->operation options count-pred)
          pruned-operations (prune-operations out-fields grouped options)]
      (is (= 3 (count pruned-operations)))
      (is (contains-op? minus-op pruned-operations))
      (is (contains-op? plus-op pruned-operations))
      (is (contains-op? count-op pruned-operations)))

    "Do not prune if predicate outvar is used in an option (:sort)"
    (let [{:keys [grouped options]} (mk-grouped-and-options [gen-pred minus-pred plus-pred sort-pred])
          out-fields ["?minus"]
          minus-op (predicate->operation options minus-pred)
          plus-op (predicate->operation options plus-pred)
          pruned-operations (prune-operations out-fields grouped options)]
      (is (= 2 (count pruned-operations)))
      (is (contains-op? minus-op pruned-operations))
      (is (contains-op? plus-op pruned-operations)))

    "Do not prune if predicate outvar is used in generators field (ie, a join)"
    (let [join-gen-pred [[[3 "a"][7 "b"]] :> "?plus" "!!alpha"]
          {:keys [grouped options]} (mk-grouped-and-options [gen-pred minus-pred plus-pred join-gen-pred])
          out-fields ["?minus" "!!alpha"]
          minus-op (predicate->operation options minus-pred)
          plus-op (predicate->operation options plus-pred)
          pruned-operations (prune-operations out-fields grouped options)]
      (is (= 2 (count pruned-operations)))
      (is (contains-op? minus-op pruned-operations))
      (is (contains-op? plus-op pruned-operations)))))
