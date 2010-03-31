(ns cascalog.util)

(defn transpose [m]
  (apply map list m))

(defn substitute-if
  "Returns [newseq {map of newvals to oldvals}]"
  [pred subfn aseq]
  (reduce (fn [[newseq subs] val]
    (let [[newval sub] (if (pred val)
            (let [subbed (subfn val)] [subbed {subbed val}])
            [val {}])]
      [(conj newseq newval) (merge subs sub)]))
    [[] {}] aseq))

(defn try-resolve [obj]
  (when (symbol? obj) (resolve obj)))