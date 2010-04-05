(ns cascalog.graph-test
  (:use clojure.test)
  (:use cascalog.graph))

(set! *print-level* 3)

(defn outnodevals= [n vals]
  (= (set (map get-value (get-outbound-nodes n))) (set vals)))

(deftest test-add-nodes-and-edges
  (let [g (mk-graph)
        n1 (create-node g "AAA")
        n2 (create-node g "BBB")
        n3 (create-node g "CCC")
        n4 (create-node g "DDD")]
        (create-edge n1 n2)
        (create-edge n1 n3)
        (create-edge n3 n4)
        (is (= "AAA" (get-value n1)))
        (is (= "CCC" (get-value n3)))
        (is (outnodevals= n1 ["BBB" "CCC"]))
        (is (outnodevals= n2 []))
        (is (outnodevals= n3 ["DDD"]))
        (is (outnodevals= n4 []))
      ))

(deftest test-extra-data
   (let [g (mk-graph)
        n1 (create-node g "n1")
        n2 (create-node g "n2")
        e (create-edge n1 n2)]
        (add-extra-data e :a 1)
        (add-extra-data n1 :a 2)
        (add-extra-data n1 :b 5)
        (add-extra-data n2 :b 3)
        (is (= 2 (get-extra-data n1 :a)))
        (is (= 5 (get-extra-data n1 :b)))
        (is (= nil (get-extra-data n1 :c)))
        (is (= 1 (get-extra-data e :a)))
        (add-extra-data e :a 101)
        (update-extra-data n2 :b inc)
        (is (= 4 (get-extra-data n2 :b)))
        (is (= 101 (get-extra-data (first (get-outbound-edges n1)) :a)))
      ))

