(ns cascalog.in-memory.util)

(defn smallest-arity [fun]
  "Returns the smallest number of arguments the function takes"
  (->> fun meta :arglists first count))

(defn system-println [s]
  (.println System/out s))
