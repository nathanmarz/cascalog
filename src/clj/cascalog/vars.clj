(ns cascalog.vars)

(defn cascalog-var? [obj]
    (if (or (symbol? obj) (string? obj))
      ((complement nil?) (some #(.startsWith (str obj) %) ["?" "!" "!!"]))
      false ))

(defn non-nullable-var? [sym-or-str]
  (.startsWith (str sym-or-str) "?"))

(def nullable-var? (complement non-nullable-var?))

(defn vars2str [vars]
  (map #(if (cascalog-var? %) (str %) %) vars))