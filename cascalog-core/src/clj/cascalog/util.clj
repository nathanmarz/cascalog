(ns cascalog.util
  (:refer-clojure :exclude [flatten memoize])
  (:require [clojure.walk :refer (postwalk)]
            [jackknife.seq :refer (unweave)])
  (:import [java.util UUID]))

(defn ^String path
  [x]
  (if (string? x)
    x
    (.getAbsolutePath ^java.io.File x)))

(defmacro with-timeout
  "Accepts a vector with a timeout (in ms) and any number of forms and
  executes those forms sequentially. returns the result of the last
  form or nil (if the timeout is reached.) For example:

  (with-timeout [100]
    (Thread/sleep 50)
    \"done!\")
  ;;=> \"done!\"

  (with-timeout [100]
    (Thread/sleep 200)
    \"done!\")
  ;;=> nil"
  [[ms] & body]
  `(let [^java.util.concurrent.Future f# (future ~@body)]
     (try (.get f# ~ms java.util.concurrent.TimeUnit/MILLISECONDS)
          (catch java.util.concurrent.TimeoutException e#
            (.cancel f# true)
            nil))))

(defn flatten
  "Flattens out a nested sequence. unlike clojure.core/flatten, also
  flattens maps."
  [vars]
  (->> vars
       (postwalk #(if (map? %) (seq %) %))
       (clojure.core/flatten)))

(defn multifn? [op]
  (instance? clojure.lang.MultiFn op))

(defn multi-set
  "Returns a map of elem to count"
  [aseq]
  (apply merge-with +
         (map #(hash-map % 1) aseq)))

(defn uuid []
  (str (UUID/randomUUID)))

(defn unique-rooted-paths
  "Returns an infinite* sequence of unique subpaths of the supplied
  root path."
  [root]
  (repeatedly #(str root "/" (uuid))))

(defn all-pairs
  "[1 2 3] -> [[1 2] [1 3] [2 3]]"
  [coll]
  (let [pair-up (fn [v vals]
                  (map (partial vector v) vals))]
    (apply concat (for [i (range (dec (count coll)))]
                    (pair-up (nth coll i) (drop (inc i) coll))))))

(defn reverse-map
  "{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"
  [amap]
  (reduce (fn [m [k v]]
            (let [existing (get m v [])]
              (assoc m v (conj existing k))))
          {} amap))

(defn count= [& args]
  (apply = (map count args)))

(def not-count=
  (complement count=))

(defn- clean-nil-bindings [bindings]
  (let [pairs (partition 2 bindings)]
    (mapcat identity (filter #(first %) pairs))))

(defn meta-update
  "Returns the supplied symbol with the supplied `attr` map conj-ed
  onto the symbol's current metadata."
  [sym f]
  (with-meta sym (f (meta sym))))

(defn meta-conj
  "Returns the supplied symbol with the supplied `attr` map conj-ed
  onto the symbol's current metadata."
  [sym attr]
  (meta-update sym (fn [m] (if m (conj m attr) attr))))

(defn set-namespace-value
  "Merges the supplied kv-pair into the metadata of the namespace in
  which the function is called."
  [key-name newval]
  (alter-meta! *ns* merge {key-name newval}))

(defn mk-destructured-seq-map
  "Accepts pairs of bindings and generates a map of replacements to
  make... TODO: More docs."
  [& bindings]
  ;; lhs needs to be symbolified
  (let [bindings (clean-nil-bindings bindings)
        to-sym (fn [s] (if (keyword? s) s (symbol s)))
        [lhs rhs] (unweave bindings)
        lhs (for [l lhs] (if (sequential? l)
                           (vec (map to-sym l))
                           (symbol l)))
        rhs (for [r rhs] (if (sequential? r)
                           (vec r)
                           r))
        destructured (vec (destructure (interleave lhs rhs)))
        syms (first (unweave destructured))
        extract-code (vec (for [s syms] [(str s) s]))]
    (eval
     `(let ~destructured
        (into {} ~extract-code)))))

(defn memoize
  "Similar to clojure.core/memoize, but allows for an optional
  initialization map."
  {:static true}
  ([f] (memoize f {}))
  ([f init-m]
     (let [mem (atom init-m)]
       (fn [& args]
         (if-let [e (find @mem args)]
           (val e)
           (let [ret (apply f args)]
             (swap! mem assoc args ret)
             ret))))))
