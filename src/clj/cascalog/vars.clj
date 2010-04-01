(ns cascalog.vars)

(defn- extract-varname [v]
  (if (symbol? v) (name v) v))

(defn cascalog-var? [obj]
    (if (or (symbol? obj) (string? obj))
      (let [obj (extract-varname obj)]
        ((complement nil?) (some #(.startsWith obj %) ["?" "!" "!!"])))
        false ))

(defn non-nullable-var? [sym-or-str]
  (.startsWith (extract-varname sym-or-str) "?"))

(def nullable-var? (complement non-nullable-var?))

(defn unground-var?
  "!! vars that cause outer joins"
  [sym-or-str]
  (.startsWith (extract-varname sym-or-str) "!!"))

(defn vars2str [vars]
  (vec (map #(if (cascalog-var? %) (extract-varname %) %) vars)))

;; TODO: better to use UUIDs to avoid name collisions with client code?
;; Are the size of fields an issue in the actual flow execution perf-wise?
(let [i (atom 0)]
  (defn gen-unique-suffix [] (str "__gen" (swap! i inc))))

(defn gen-nullable-var [] (str "!" (gen-unique-suffix)))

(defn uniquify-vars
  "Uniques the cascalog vars according to mappings, generates new names if no mapping"
  [vars mappings]
  (reduce (fn [[p m] v]
            (if (cascalog-var? v)
              (let [newv (or (m v) (str v (gen-unique-suffix)))]
                [(conj p newv) (assoc m v newv)])
              [(conj p v) m] ))
    [[] mappings] vars))