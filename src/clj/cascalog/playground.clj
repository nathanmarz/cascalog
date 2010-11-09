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

(ns cascalog.playground
  (:use [cascalog api testing]))

(defmacro bootstrap []
  '(do
    (use (quote [clojure.contrib.seq-utils :only [find-first]]))
    (use (quote cascalog.api))
    (require (quote [cascalog [workflow :as w] [ops :as c]]))
    ))

(defmacro bootstrap-emacs []
  '(do
    (use (quote [clojure.contrib.seq-utils :only [find-first]]))
    (use (quote cascalog.api))
    (require (quote [cascalog [workflow :as w] [ops :as c]]))

    (import (quote [java.io PrintStream]))
    (import (quote [cascalog WriterOutputStream]))
    (import (quote [org.apache.log4j Logger WriterAppender SimpleLayout]))
    (.addAppender (Logger/getRootLogger) (WriterAppender. (SimpleLayout.) *out*))
    (System/setOut (PrintStream. (WriterOutputStream. *out*)))
    ))

(def person (memory-source-tap [
  ["alice"]
  ["bob"]
  ["chris"]
  ["david"]
  ["emily"]
  ["george"]
  ["gary"]
  ["harold"]
  ["kumar"]
  ["luanne"]
  ]))

(def age (memory-source-tap [
  ["alice" 28]
  ["bob" 33]
  ["chris" 40]
  ["david" 25]
  ["emily" 25]
  ["george" 31]
  ["gary" 28]
  ["kumar" 27]
  ["luanne" 36]
  ]))

(def gender (memory-source-tap [
  ["alice" "f"]
  ["bob" "m"]
  ["chris" "m"]
  ["david" "m"]
  ["emily" "f"]
  ["george" "m"]
  ["gary" "m"]
  ["harold" "m"]
  ["luanne" "f"]
  ]))

(def location (memory-source-tap [
  ["alice" "usa" "california" nil]
  ["bob" "canada" nil nil]
  ["chris" "usa" "pennsylvania" "philadelphia"]
  ["david" "usa" "california" "san francisco"]
  ["emily" "france" nil nil]
  ["gary" "france" nil "paris"]
  ["luanne" "italy" nil nil]
  ]))

(def follows (memory-source-tap [
  ["alice" "david"]
  ["alice" "bob"]
  ["alice" "emily"]
  ["bob" "david"]
  ["bob" "george"]
  ["bob" "luanne"]
  ["david" "alice"]
  ["david" "luanne"]
  ["emily" "alice"]
  ["emily" "bob"]
  ["emily" "george"]
  ["emily" "gary"]
  ["george" "gary"]
  ["harold" "bob"]
  ["luanne" "harold"]
  ["luanne" "gary"]
  ]))

(def num-pair (memory-source-tap [
  [1 2]
  [0 0]
  [1 1]
  [4 4]
  [5 10]
  [2 7]
  [3 6]
  [8 64]
  [8 3]
  [4 0]
  ]))

(def integer (memory-source-tap [
  [-1]
  [0]
  [1]
  [2]
  [3]
  [4]
  [5]
  [6]
  [7]
  [8]
  [9]
  ]))

(def sentence (memory-source-tap [
  ["Four score and seven years ago our fathers brought forth on this continent a new nation"]
  ["conceived in Liberty and dedicated to the proposition that all men are created equal"]
  ["Now we are engaged in a great civil war testing whether that nation or any nation so"]
  ["conceived and so dedicated can long endure We are met on a great battlefield of that war"]
  ["We have come to dedicate a portion of that field as a final resting place for those who"]
  ["here gave their lives that that nation might live It is altogether fitting and proper"]
  ["that we should do this"]
  ["But in a larger sense we can not dedicate  we can not consecrate  we can not hallow"]
  ["this ground The brave men living and dead who struggled here have consecrated it"]
  ["far above our poor power to add or detract The world will little note nor long remember"]
  ["what we say here but it can never forget what they did here It is for us the living rather"]
  ["to be dedicated here to the unfinished work which they who fought here have thus far so nobly"]
  ["advanced It is rather for us to be here dedicated to the great task remaining before us "]
  ["that from these honored dead we take increased devotion to that cause for which they gave"]
  ["the last full measure of devotion  that we here highly resolve that these dead shall"]
  ["not have died in vain  that this nation under God shall have a new birth of freedom"]
  ["and that government of the people by the people for the people shall not perish"]
  ["from the earth"]
  ]))

(def dirty-ages (memory-source-tap [
; [timestamp name age]
  [1200 "alice" 20]
  [1000 "bob" 25]
  [1500 "harry" 46]
  [1800 "alice" 19]
  [2000 "bob" 30]
  ]))

(def dirty-follower-counts (memory-source-tap [
; [timestamp name follower-count]
  [2000 "gary" 56]
  [1100 "george" 124]
  [1900 "gary" 49]
  [3000 "juliette" 1002]
  [3002 "juliette" 1010]
  [3001 "juliette" 1011]
  ]))

