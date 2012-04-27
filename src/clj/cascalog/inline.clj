(ns cascalog.inline
  (:require [clojure.set :as set])
  (:require [cascalog.vars :as v]))


(defn- leaves [form]
  (if (coll? form)
    (reduce (fn [curr elem] (set/union curr (leaves elem)))
            #{}
            form)
  #{form}))

(defn inlineop-builder [op-sym body]
  (let [used-vars (->> body leaves (filter symbol?) (filter v/cascalog-var?))
        impl `(~op-sym ~(vec used-vars) ~@body)]
    `(cascalog.api/predmacro [invars# outvars#]
      {:pre [(zero? (count invars#))]}
      [[~impl :<< ~(vec (map str used-vars)) :>> outvars#]]
      )))
