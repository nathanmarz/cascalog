(ns cascalog.logic.vars-test
  (:use midje.sweet
        cascalog.logic.vars))

(facts
  "Strings and symbols work as vars"
  ["?a" '?a '?face_two '!!two '!a] => (has every? cascalog-var?)

  "The underscore isn't a cascalog-var,"
  ['_ "_"] =not=> (has some cascalog-var?)

  "But it is reserved, along with & (for predmacros)."
  ['_ "_" '& "&"] => (has every? reserved?)

  "Unground vars begin with !!"
  '!!a => unground-var?

  "! and ? vars are ground."
  ['!a '?a] => (has every? ground-var?)

  "Adding !!a causes the test to fail."
  ['!!a '!a '?a] =not=> (has every? ground-var?)

  "A sequence of vars is only fully ground if every var is ground."
  ['?b '!a '?a] => fully-ground?

  "As before, the addition of an unground var ungrounds the sequence."
  ['!!b '!a '?a] =not=> fully-ground?)


(fact
  "with-logic-vars allows logic symbols to be used without quoting."
  (with-logic-vars
    (str !!d ?a ?b "see!") => "!!d?a?bsee!"))

(let [non-nullables (gen-non-nullable-vars 10)
      nullables     (gen-nullable-vars 10)]
  (fact
    "The non-nullable generator generates non-nullable vars..."
    non-nullables => (has every? non-nullable-var?)

    "And no nullables."
    non-nullables =not=> (has some nullable-var?)

    "The non-nullable generator generates non-nullable vars..."
    nullables => (has every? nullable-var?)

    "And no nullables."
    nullables =not=> (has some non-nullable-var?)))

(fact
  "Sanitize replaces cascalog variables with strings and munges
  underscores. The replaced underscore is unground if any of the
  replaced variables are unground."
  (sanitize [* '!!a '?b '_ '& :> 10]) => (fn [result]
                                           (let [ignored (nth result 3)]
                                             (and (cascalog-var? ignored)
                                                  (unground-var? ignored)
                                                  (= result [* "!!a" "?b" ignored "&" :> 10]))))

  "Sanitize also works with deeply nested structures."
  (sanitize
   {:key1 [['?a] '_ '&]
    :key2 ['othersym [] '?b]})
  => (fn [result]
       (let [ignored (-> result :key1 second)]
         (and (cascalog-var? ignored)
              (ground-var? ignored)
              (= result {:key1 [["?a"] ignored "&"]
                         :key2 ['othersym [] "?b"]})))))
