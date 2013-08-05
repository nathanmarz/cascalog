(ns cascalog.logic.def-test
  (:use midje.sweet
        cascalog.logic.def))

(defn square [x]
  (* x x))

(defn sum [& xs]
  (reduce + xs))

(defmapfn plus-two [x]
  (+ 2 x))

(defn times
  [y]
  (mapfn [x] (* x y)))

(facts
  "Normal squaring function works."
  (square 10) => 100

  "And still works as a mapop. The behavior is unchanged."
  ((mapop square) 3) => 9

  "anonymous mapops work as functions"
  ((mapfn [x] (* x 5)) 4) => 20

  "operations defined with def*fn work as normal functions."
  (plus-two 2) => 4

  "Higher order mapfns work normally"
  ((times 2) 4) => 8)
