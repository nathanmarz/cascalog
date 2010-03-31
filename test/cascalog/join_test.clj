; (ns cascading.clojure.join-test
;   (:use clojure.test
;         clojure.contrib.java-utils
;         cascading.clojure.testing)
;   (:import (cascading.tuple Fields)
;            (cascading.pipe Pipe)
;            (cascading.clojure Util ClojureMap))
;   (:require (cascading.clojure [api :as c])))
; 
; (deftest inner-join-test
;   (test-flow
;    (in-pipes {"lhs" ["name" "num"]
;         "rhs" ["name" "num"]})
;    (in-tuples {"lhs" [["foo" 5] ["bar" 6]]
;          "rhs" [["foo" 1] ["bar" 2]]})
;    (fn [{lhs "lhs" rhs "rhs"}]
;      (-> [lhs rhs]
;    (c/inner-join
;           [["name"] ["name"]]
;           ["name1" "val1" "nam2" "val2"])
;    (c/select ["val1" "val2"])))
;    [[6 2] [5 1]]))
; 
; (deftest multi-pipe-inner-join-test
;   (test-flow
;    (in-pipes {"p1" ["name" "num"]
;         "p2" ["name" "num"]
;         "p3" ["name" "num"]})
;    (in-tuples {"p1" [["foo" 5] ["bar" 6]]
;          "p2" [["foo" 1] ["bar" 2]]
;          "p3" [["foo" 7] ["bar" 8]]})
;    (fn [{p1 "p1" p2 "p2" p3 "p3"}]
;      (-> [p1 p2 p3]
;    (c/inner-join
;           [["name"] ["name"] ["name"]]
;           ["name1" "val1" "name2" "val2" "name3" "val3"])
;    (c/select ["val1" "val2" "val3"])))
;    [[6 2 8] [5 1 7]]))
; 
; (deftest multi-pipe-multi-field-inner-join-test
;   (test-flow
;    (in-pipes {"p1" ["x" "y" "num"]
;         "p2" ["x" "y" "num"]
;         "p3" ["x" "y" "num"]})
;    (in-tuples {"p1" [[0 1 5] [2 1 6]]
;          "p2" [[0 1 1] [2 1 2]]
;          "p3" [[2 1 7] [0 1 8]]})
;    (fn [{p1 "p1" p2 "p2" p3 "p3"}]
;      (-> [p1 p2 p3]
;    (c/inner-join
;           [["x" "y"]["x" "y"]["x" "y"]]
;           ["x1" "y1" "val1" "x2" "y2" "val2" "x3" "y3" "val3"])
;    (c/select ["val1" "val2" "val3"])))
;    [[5 1 8] [6 2 7]]))
; 
; (deftest multi-pipe-outer-join-test
;   (test-flow
;    (in-pipes {"p1" ["name" "num"]
;         "p2" ["name" "num"]
;         "p3" ["name" "num"]})
;    (in-tuples {"p1" [["foo" 5] ["bar" 6]]
;          "p2" [["baz" 1] ["foo" 2]]
;          "p3" [["bar" 7] ["baz" 8]]})
;    (fn [{p1 "p1" p2 "p2" p3 "p3"}]
;      (-> [p1 p2 p3]
;    (c/outer-join
;           [["name"] ["name"] ["name"]]
;           ["name1" "val1" "name2" "val2" "name3" "val3"])
;    (c/select ["val1" "val2" "val3"])))
;    [[6 nil 7] [nil 1 8] [5 2 nil]]))
