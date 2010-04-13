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

(ns cascalog.workflow-example
  (:import (cascading.tuple Fields))
  (:require (cascalog [workflow :as w])))


; (w/defassembly c-distinct [pipe]
;   (pipe (w/group-by Fields/ALL) (w/first)))


; (defn starts-with-b? [word]
;   (re-find #"^b.*" word))

; (defn split-words
;   {:fields "word"}
;   [line]
;   (re-seq #"\w+" line))

; (defn uppercase [word]
;   (.toUpperCase word))

(w/defmapop [insert [v]] "val" [& _]
  v )

(w/defbufferop my-sum "count" [numbers]
  [(reduce + (map first numbers))])

(w/deffilterop [starts-with? [s]] [word]
  (re-find (re-pattern (str "^" s ".*")) word))

(w/defmapcatop split-words "word" [line]
  (re-seq #"\w+" line))

(w/defmapop uppercase [word]
  (.toUpperCase word))

;(w/defassembly example-assembly [phrase white]
;  [phrase (phrase (split-words "line")
;                  (starts-with? ["b"])
;                  (w/group-by "word")
;                  (w/count "count"))
;   white (white (split-words "line" :fn> "white"))]
;   ([phrase white] (w/inner-join ["word" "white"] ["word" "count" "white"])
;                   (w/select ["word" "count"])
;                 (uppercase "word" :fn> "upword" :> ["upword" "count"] )))

(w/defassembly example-assembly [words]
    (words (split-words "line")
           (insert [1] :fn> "val" :> ["word" "val"])
           (w/group-by "word")
           (my-sum "val" :fn> "count")
           (w/select ["word" "count"])))


(defn run-example
  [in-phrase-dir-path in-white-dir-path out-dir-path]
  (let [source-scheme  (w/text-line "line")
        sink-scheme    (w/text-line ["word" "count"])
        phrase-source  (w/hfs-tap source-scheme in-phrase-dir-path)
;        white-source   (w/hfs-tap source-scheme in-white-dir-path)
        sink           (w/hfs-tap sink-scheme out-dir-path)
        flow           (w/mk-flow [phrase-source] sink example-assembly)]
    (w/exec flow)))

(comment
  (use 'cascading.clojure.api-example)
  (def root "/Users/marz/opensource/cascading-clojure/")
  (def example-args
    [(str root "data/phrases")
     (str root "data/white")
     (str root "data/output")])
  (apply run-example example-args)
)
