(ns cascalog.fluent.cascading
  (:require [clojure.set :refer (subset?)]
            [jackknife.seq :refer (collectify)])
  (:import [cascading.tap Tap]
           [cascading.tuple Fields]
           [cascading.pipe Pipe]))

(let [i (atom 0)]
  ;; TODO: Is it better to use UUIDs to avoid name collisions with
  ;; client code?  Are the size of fields an issue in the actual flow
  ;; execution perf-wise?
  (defn gen-unique-suffix []
    (str "__gen" (swap! i inc))))

(defn gen-var-fn
  "Accepts a prefix and returns a function of no arguments that, when
  called, produces a unique string with the supplied prefix."
  [prefix]
  (fn [] (str prefix (gen-unique-suffix))))

(def gen-unique-var
  "Returns a unique non-nullable var with a optional suffix."
  (gen-var-fn ""))

(defn ^Fields fields
  "Returns the supplied object as an instance of Cascading Fields."
  [x]
  (cond (instance? Fields x) x
        (nil? x) Fields/NONE
        :else (let [coll (collectify x)]
                (if (empty? coll)
                  Fields/NONE
                  (->> coll
                       (map str)
                       (into-array String)
                       (Fields.))))))

(defn fields-array
  [fields-seq]
  (into-array Fields (map fields fields-seq)))

(defn pipes-array
  [pipes]
  (into-array Pipe pipes))

(defn taps-array
  [taps]
  (into-array Tap taps))

(defn generic-cascading-fields?
  [cfields]
  (or (.isSubstitution cfields)
      (.isUnknown cfields)
      (.isArguments cfields)
      (.isResults cfields)
      (.isSwap cfields)
      (.isReplace cfields)))

(defn default-output
  "Returns a default output field vector for operations that accept
  from-fields and return to-fields."
  [from-fields to-fields]
  (let [from (into #{} from-fields)
        to   (into #{} to-fields)]
    (cond
     ;; If the op returns Fields/ARGS, replace the input.
     (.isArguments to-fields) Fields/REPLACE

     ;; zero size is Fields/ALL, Fields/UNKNOWN, etc
     (zero? (.size from-fields)) Fields/ALL

     ;; If the fields are equal, replace the input with output.
     (and (= from to)) Fields/REPLACE

     ;; The output is a superset and replaces the input, OR the output
     ;; is a subset and throws away some input.
     (or (subset? from to) (subset? to from)) Fields/SWAP

     ;; Either the sets are disjoint (in which case everything is
     ;; fine) or the intersection is strange and cascading will throw.
     :else Fields/ALL)))
