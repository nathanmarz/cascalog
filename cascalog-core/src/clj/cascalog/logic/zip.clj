(ns cascalog.logic.zip
  (:require [clojure.zip :as zip]
            [jackknife.seq :refer (collectify)]))

(defprotocol TreeNode
  (branch? [node] "Is it possible for node to have children?")
  (children [node] "Return children of this node.")
  (make-node [node children] "Makes new node from existing node and new children."))

(extend-protocol TreeNode
  Object
  (branch? [node]
    false)
  (make-node [node children] node))

(defn cascalog-zip
  "Returns a zipper for cascalog nodes, given a root sequence"
  [root]
  (zip/zipper branch? children make-node root))

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
  (if (zip/end? loc) ;; If it's the end, return the end.
    loc
    (if (nil? (zip/up loc))
      [(zip/node loc) :end]
      (or (and (zip/right loc) (leftmost-descendant (zip/right loc)))
          (zip/up loc)))))

(defn postwalk-edit [zipper matcher editor & {:keys [encoder]
                                              :or {encoder identity}}]
  (loop [visited {}
         loc (leftmost-descendant zipper)]
    (if (zip/end? loc)
      (zip/root loc)
      (if-let [res (visited (encoder (zip/node loc)))]
        (recur visited (my-next (zip/replace loc res)))
        (if-let [matcher-result (matcher (zip/node loc))]
          (let [res (editor matcher-result (zip/node loc))]
            (recur (assoc visited (encoder (zip/node loc)) res)
                   (my-next (zip/replace loc res))))
          (recur visited (my-next loc)))))))

(comment
  "Example of how zippers can be used to walk a map:"
  (extend-protocol TreeNode
    clojure.lang.IPersistentMap
    (branch? [node] true)
    (children [node]
      (collectify (:children node)))
    (make-node [node children]
      (with-meta children (meta node))))

  (let [a {:children [1 2 3]}
        b {:children [a {:children [4 5]}]}
        c {:children [a {:children [8 9]}]}]
    (postwalk-edit (cascalog-zip {:children [b c]})
                   identity
                   (fn [x _] (do (println x) (if (number? x) (inc x) x)))))

  {:children [{:children [1 2 3]}
              {:children [4 5]}]}
  (-> (cascalog-zip {:children [{:children [1 2 3]}
                                {:children [4 5]}]})
      zip/down
      zip/up
      zip/down
      zip/right
      zip/node))
