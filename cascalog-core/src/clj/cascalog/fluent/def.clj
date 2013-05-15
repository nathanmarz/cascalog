(ns cascalog.fluent.def
  (:require [jackknife.core :refer (safe-assert)]
            [clojure.tools.macro :refer (name-with-attributes)]
            [cascalog.util :as u]))

(defn- update-arglists
  "Scans the forms of a def* operation and adds an appropriate
  `:arglists` entry to the supplied `sym`'s metadata."
  [sym [form :as args]]
  (let [arglists (if (vector? form)
                   (list form)
                   (clojure.core/map clojure.core/first args))]
    (u/meta-conj sym {:arglists (list 'quote arglists)})))

(defn- update-fields
  "Examines the first item in a def* operation's forms. If the first
  form defines a sequence of Cascading output fields, these are added
  to the supplied `sym`'s metadata and dropped from the form
  sequence. Else, `sym` and `forms` are left unchanged.

  This function will no longer be necessary, if Cascalog deprecates
  the ability to name output fields before the dynamic argument
  vector."
  [sym [form :as forms]]
  (if (string? (clojure.core/first form))
    [(u/meta-conj sym {:fields form}) (rest forms)]
    [sym forms]))

(defn assert-nonvariadic [args]
  (safe-assert (not (some #{'&} args))
               (str "Defops currently don't support variadic arguments.\n"
                    "The following argument vector is invalid: " args)))

(defn- parse-defop-args
  "Accepts a def* type and the body of a def* operation binding,
  outfits the function var with all appropriate metadata, and returns
  a 3-tuple containing `[fname f-args args]`.

  * `fname`: the function var.
  * `f-args`: static variable declaration vector.
  * `args`: dynamic variable declaration vector."
  [type [spec & args]]
  (let [[fname f-args] (if (sequential? spec)
                         [(clojure.core/first spec) (second spec)]
                         [spec nil])
        [fname args] (->> [fname args]
                          (apply name-with-attributes)
                          (apply update-fields))
        fname (update-arglists fname args)
        fname (u/meta-conj fname {:pred-type (keyword (name type))
                                  :hof? (boolean f-args)})]
    (assert-nonvariadic f-args)
    [fname f-args args]))

(defn- defop-helper
  "Binding helper for cascalog def* ops. Function value is tagged with
   appropriate cascalog metadata; metadata can be accessed with `(meta
   op)`, rather than the previous `(op :meta)` requirement. This is so
   you can pass operations around and dynamically create flows."
  [type args]
  (let  [[fname func-args funcdef] (parse-defop-args type args)
         args-sym        (gensym "args")
         args-sym-all    (gensym "argsall")
         runner-name     (symbol (str fname "__"))
         _ (prn runner-name)
         func-form       (if func-args
                           `[(var ~runner-name) ~@func-args]
                           `(var ~runner-name))
         runner-body     (if func-args
                           `(~func-args (fn ~@funcdef))
                           funcdef)
         assembly-args   (if func-args
                           `[~func-args & ~args-sym]
                           `[ & ~args-sym])]
    (prn `(do (defn ~runner-name
           ~(assoc (meta fname)
              :no-doc true
              :skip-wiki true)
           ~@runner-body)
         (def ~fname
           (with-meta
             (fn [& ~args-sym-all]
               (let [~assembly-args ~args-sym-all]
                 (apply ~type ~func-form ~args-sym)))
             ~(meta fname)))))
    `(do (defn ~runner-name
           ~(assoc (meta fname)
              :no-doc true
              :skip-wiki true)
           ~@runner-body)
         (def ~fname
           (with-meta
             (fn [& ~args-sym-all]
               (let [~assembly-args ~args-sym-all]
                 (apply ~type ~func-form ~args-sym)))
             ~(meta fname))))))

(defmacro defmapop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.fluent.workflow/map args))

(defmacro defmapcatop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.fluent.workflow/mapcat args))

(defmacro deffilterop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.fluent.workflow/filter args))

(defmacro defaggregateop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.fluent.workflow/aggregate args))

(defmacro defbufferop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.fluent.workflow/buffer args))

(defmacro defmultibufferop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.fluent.workflow/multibuffer args))

(defmacro defbufferiterop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.fluent.workflow/bufferiter args))
