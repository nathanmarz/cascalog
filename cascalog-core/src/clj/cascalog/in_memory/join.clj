(ns cascalog.in-memory.join
  (:require [cascalog.in-memory.tuple :refer (to-tuple empty-tuple)]))

(defn inner-join
  "Inner joins two maps that both have been grouped by the same function.
   This is an inner join, so nils are discarded."
  [l-grouped r-grouped]
  (->> l-grouped
      (map
       (fn [l-group]
         (let [[k l-tuples] l-group]
           (if-let [r-tuples (get r-grouped k)]
             (for [x l-tuples y r-tuples]
               (merge x y))))))
      (remove nil?)
      flatten))

(defn left-join
  "Joins two maps (a left and a right) that have been grouped by
   the same function. Keeps only values found on the left and
   returns nil for values not found on the right."
  [l-grouped r-grouped r-fields]
  (->> l-grouped
       (map
        (fn [l-group]
          (let [[k l-tuples] l-group
                r-empty-tuples [(empty-tuple r-fields)]
                r-tuples (get r-grouped k r-empty-tuples)]
            (for [x l-tuples y r-tuples]
                ;; merge is specifically ordered, because the left
                ;; tuple takes precedence over the right one (which
                ;; could be nil)
                (merge y x)))))
       (remove nil?)
       flatten))

(defn left-existence-join
  "Similar to a left-join except it includes an additional argument,
  existence-field, that captures the boolean about whether a join was
  found or not.  True if a left value was found.  False if not."  
  [l-grouped r-grouped r-fields existence-field]
  (->> l-grouped
       (map
        (fn [l-group]
          (let [[k l-tuples] l-group
                r-empty-tuples [(empty-tuple r-fields)]
                r-tuple (first (get r-grouped k r-empty-tuples))
                existence-tuple (if (contains? r-grouped k)
                                  (to-tuple [existence-field] [true])
                                  (to-tuple [existence-field] [false]))]
            (for [x l-tuples]
                ;; merge is specifically ordered, because the left
                ;; tuple takes precedence over the right one (which
                ;; could be nil)
                (merge existence-tuple r-tuple x)))))
       (remove nil?)
       flatten))

(defn left-excluding-join
  "A left join that only returns values where the right side is nil"
  [l-grouped r-grouped r-fields]
  (->> l-grouped
       (map
        (fn [l-group]
          (let [[k l-tuples] l-group
                r-empty-tuple (empty-tuple r-fields)]
            (if (not (find r-grouped k))
              (map #(merge r-empty-tuple %) l-tuples)))))
       (remove nil?)
       flatten))

(defn outer-join
  "A join that contains all of the values between the two maps,
  but none duplicated"
  [l-grouped r-grouped l-fields r-fields]
  (let [inner (inner-join l-grouped r-grouped)
        left (left-excluding-join l-grouped r-grouped r-fields)
        right (left-excluding-join r-grouped l-grouped l-fields)]
    (concat inner left right)))

(defn join
  "Dispatches to all the different join types and returns a vector
   with the collection of joined tuples and the type of join"
  [l-grouped r-grouped l-type r-type l-fields r-fields]
  (cond
   (and (= :inner l-type) (= :inner r-type))
   [(inner-join l-grouped r-grouped) :inner]
   (and (= :inner l-type) (= :outer r-type))
   [(left-join l-grouped r-grouped r-fields) :outer]
   (= :inner l-type)
   [(left-existence-join l-grouped r-grouped r-fields r-type) :outer]
   (and (= :outer l-type) (= :inner r-type))
   [(left-join r-grouped l-grouped l-fields) :outer]
   (and (= :outer l-type) (= :outer r-type))
   [(outer-join l-grouped r-grouped l-fields r-fields) :outer]
   :else
   [(left-existence-join l-grouped r-grouped r-fields r-type) :outer]))
