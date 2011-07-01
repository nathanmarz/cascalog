 ;    Copyright 2010 Nathan Marz
 ; 
 ;    This program is free software: you can redistribute it and/or modify
 ;    it under the terms of the GNU General Public License as published by
 ;    the Free Software Foundation, either version 3 of the License, or
 ;    (at your option) any later version.
 ; 
 ;    This program is distributed in the hope that it will be useful,
 ;    but WITHOUT ANY WARRANTY; without even the implied warranty of
 ;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 ;    GNU General Public License for more details.
 ; 
 ;    You should have received a copy of the GNU General Public License
 ;    along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns cascalog.util
  (:use [clojure.contrib.seq-utils :only [find-first indexed]])
  (:import [java.util UUID Collection]))

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
  (if (or (sequential? obj) (instance? Collection obj)) obj [obj]))

(defn multi-set
  "Returns a map of elem to count"
  [aseq]
  (apply merge-with +
         (map #(hash-map % 1) aseq)))

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
  (when (odd? (count coll))
    (throw (IllegalArgumentException. "Need even number of args to unweave")))
  [(take-nth 2 coll) (take-nth 2 (rest coll))])


(defn pairs2map [pairs]
  (apply hash-map (flatten pairs)))

(defn reverse-map
  "{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"
  [amap]
  (reduce (fn [m [k v]]
    (let [existing (get m v [])]
      (assoc m v (conj existing k))))
    {} amap))

(defn some? [pred coll]
  ((complement nil?) (some pred coll)))

(defmacro dofor [& body]
  `(doall (for ~@body)))

(defn count= [& args]
  (apply = (map count args)))

(defn not-count= [& args]
  (not (apply count= args)))

(defmacro if-ret [form else-form]
  `(if-let [ret# ~form]
    ret#
    ~else-form ))

(defn- clean-nil-bindings [bindings]
  (let [pairs (partition 2 bindings)]
    (mapcat identity (filter #(first %) pairs))
    ))

(defn mk-destructured-seq-map [& bindings]
  ;; lhs needs to be symbolified
  (let [bindings (clean-nil-bindings bindings)
        to-sym (fn [s] (if (keyword? s) s (symbol s)))
        [lhs rhs] (unweave bindings)
        lhs  (for [l lhs] (if (sequential? l) (vec (map to-sym l)) (symbol l)))
        rhs (for [r rhs] (if (sequential? r) (vec r) r))
        destructured (vec (destructure (interleave lhs rhs)))
        syms (first (unweave destructured))
        extract-code (vec (for [s syms] [(str s) s]))]
    (eval
     `(let ~destructured
        (into {} ~extract-code)
        ))
    ))
