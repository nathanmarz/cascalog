(ns cascalog.util
  (:use [clojure.contrib.seq-utils :only [find-first indexed]])
  (:import [java.util UUID]))

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

(defn collectify [obj]
  (if (sequential? obj) obj [obj]))

(defn multi-set
  "Returns a map of elem to count"
  [aseq]
  (let [update-fn (fn [m elem]
    (let [count (inc (or (m elem) 0))]
      (assoc m elem count)))]
    (reduce update-fn {} aseq)))

(defn remove-first [f coll]
  (let [i (indexed coll)
        ri (find-first #(f (second %)) i)]
        (when-not ri (throw (IllegalArgumentException. "Couldn't find an item to remove")))
        (map second (remove (partial = ri) i))
      ))

(defn uuid []
  (str (UUID/randomUUID)))

(defn all-pairs
  "[1 2 3] -> [[1 2] [1 3] [2 3]]"
  [coll]
  (let [pair-up (fn [v vals]
                  (map (partial vector v) vals))]
    (apply concat (for [i (range (dec (count coll)))]
      (pair-up (nth coll i) (drop (inc i) coll))
    ))))

(defn unweave
  "[1 2 3 4 5 6] -> [[1 3 5] [2 4 6]]"
  [coll]
  (when (odd? (count coll)) (throw (IllegalArgumentException. "Need even number of args to unweave")))
  [(take-nth 2 coll) (take-nth 2 (rest coll))])
