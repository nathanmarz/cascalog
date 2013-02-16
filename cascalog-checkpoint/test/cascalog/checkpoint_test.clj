(ns cascalog.checkpoint-test
  (:use cascalog.checkpoint
        clojure.test))

(def sprint identity)

(deftest workflow-test
  (workflow ["/tmp/lalala"]
            aaa ([] (sprint "aaa")
                   (sprint "aaa")
                   (sprint "aaa")
                   (sprint "aaa"))
            bbb ([:deps nil]
                   (sprint "bbb"))
            ccc ([:tmp-dirs ccc-path]
                   (sprint "ccc"))
            ddd ([:deps :all]
                   (sprint "ddd"))))
