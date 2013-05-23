(ns cascalog.zip
  (:require [clojure.zip :as zip]
            [jackknife.seq :refer (collectify)]))

(defn cascalog-zip
  "Returns a zipper for cascalog nodes, given a root sequence"
  {:added "1.0"}
  [root]
  (zip/zipper map?      ;; can this zipper have children?
              (comp collectify :children) ;; how to access the seq of children?

              ;; How to make a new node?
              (fn [node children]
                (with-meta children (meta node)))
              root))

(defn leftmost-descendant
  "Given a zipper loc, returns its leftmost descendent (ie, down repeatedly)."
  [loc]
  (if (and (zip/branch? loc) (zip/down loc))
    (recur (zip/down loc))
    loc))

;; Thanks, Raynes!
;;
;; https://github.com/Raynes/laser/blob/e1beb765cf40564a789fa5d2d5f795e9df724530/src/me/raynes/laser/zip.clj#L17

(defn my-next
  "Moves to the next loc in the hierarchy in postorder
   traversal. Behaves like clojure.zip/next otherwise. Note that
   unlike with a pre-order walk, the root is NOT the first element in
   the walk order, so be sure to take that into account in your
   algorithm if it matters (ie, call leftmost-descendant first thing
   before processing a node)."
  [loc]
  (if (= :end (loc 1)) ;; If it's the end, return the end.
    loc
    (if (nil? (zip/up loc))
      [(zip/node loc) :end]
      (or (and (zip/right loc) (leftmost-descendant (zip/right loc)))
          (zip/up loc)))))

(defn postwalk-edit [zipper matcher editor]
  (loop [loc (leftmost-descendant zipper)]
    (if (zip/end? loc)
      (zip/root loc)
      (if-let [matcher-result (matcher (zip/node loc))]
        (recur (my-next (zip/edit loc (partial editor matcher-result))))
        (recur (my-next loc))))))

(comment
  (-> (cascalog-zip {:children [{:children [1 2 3]}
                                {:children [4 5]}]})
      zip/down
      zip/children))
