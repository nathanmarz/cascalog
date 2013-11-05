(ns midje.cascalog
  (:use midje.sweet
        midje.cascalog.impl
        [clojure.set :only (union)]))

;; ## Custom Checkers
;;
;; Midje-Cascalog provides four custom checkers to use in your facts:
;;
;; * produces
;; * produces-some
;; * produces-prefix
;; * produces-suffix
;;
;; These act (respectively) like `just`, `contains`, `has-prefix` and
;; `has-suffix` do for normal collections. Use them like this:

;; (fact "memory sources should produce themselves."
;;   (memory-source-tap [[1]]) => (produces [[1]]))
;;
;;
;; Unlike `just` and `contains`, `produces` and `produces-some` make
;; the default assumption that output from the checked generator will
;; be unordered.
;;
;; To test for an ordered set of tuples with `produces` or
;; `produces-some`, use the `:in-order` keyword argument:
;;
;;    (produces [[10] [11]] :in-order)
;;
;; When `:in-order` is used with `produces-some`, the checker will
;; assume that gaps are okay. To test for an ordered subset of tuples,
;; use both `:in-order` and `:no-gaps` as arguments:
;;
;;    (produces-some [[10] [11]] :in-order :no-gaps)
;;
;; Using `:no-gaps` without `:in-order` is allowed but has no effect.

(defn wrap-checker
  "Accepts up to three arguments:

  `checker-fn`: a midje collection checker (just or contains, for
  example). This checker-fn will be primed with the expected set of
  tuples for the query being tested.

  `opt-map` (optional): map of some set of allowed keyword arguments
  for the checker we're generating to the corresponding arguments to
  the supplied `checker-fn`.

  `valid-set` (optional): set of options that should be allowed to
  pass on through to the wrapper checker.

  Returns a function that accepts a sequence of result tuples and
 optional arguments and returns a chatty checker tuned for said
 arguments. See `produces` and `produces-some` for usage examples."
  ([checker-fn]
     (wrap-checker checker-fn {}))
  ([checker-fn opt-map]
     (wrap-checker checker-fn opt-map #{}))
  ([checker-fn opt-map valid-set]
     {:pre [(every? set? (keys opt-map))]}
     (let [valid-opts (apply union valid-set (keys opt-map))]
       (-> (fn [& forms]
             (let [[forms opts] (split-forms forms)
                   [ll opts]    (pop-log-level opts)
                   opt-set      (mk-opt-set opts)
                   options      (opt-map opt-set [])
                   check-fn     (apply checker-fn (concat forms options))]
               (assert (valid-options? valid-opts opt-set))
               (chatty-checker
                [query]
                (check-fn (execute query :log-level ll)))))
           (with-meta {:cascalog-checker true})))))

;; With our fun function-generating-function-generating functions
;; behind us, we can move on to the meaty definitions of our Cascalog
;; collection checkers.

(def produces
  (wrap-checker just
                {#{:in-order} #{}
                 #{}          #{:in-any-order}}))

(def produces-some
  (wrap-checker contains
                {#{:in-order :no-gaps} #{}
                 #{:in-order}          #{:gaps-ok}
                 #{}                   #{:in-any-order :gaps-ok}}))

(def produces-prefix
  (wrap-checker has-prefix))

(def produces-suffix
  (wrap-checker has-suffix))

(def has-tuples
  (wrap-checker has))
