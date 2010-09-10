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

(ns cascalog.api
  (:use [cascalog vars util graph debug])
  (:use [clojure.contrib def])
  (:require cascalog.rules)
  (:require [cascalog [workflow :as w] [predicate :as p]])
  (:import [cascading.flow Flow FlowConnector])
  (:import [cascading.tuple Fields])
  (:import [cascalog StdoutTap])
  (:import [cascading.pipe Pipe]))


;; Query creation and execution

(defmacro <-
  "Constructs a query from a list of predicates."
  [outvars & predicates]
  (let [predicate-builders (vec (map cascalog.rules/mk-raw-predicate predicates))
        outvars-str (if (vector? outvars) (vars2str outvars) outvars)]
        `(cascalog.rules/build-rule ~outvars-str ~predicate-builders)))

(defn ?-
  "Executes 1 or more queries and emits the results of each query to the associated tap.
  Syntax: (?- sink1 query1 sink2 query2 ...)"
  [& bindings]
  (let [[sinks gens]    (unweave bindings)
        sourcemap       (apply merge (map :sourcemap gens))
        trapmap         (apply merge (map :trapmap gens))
        tails           (map cascalog.rules/connect-to-sink gens sinks)
        sinkmap         (w/taps-map tails sinks)
        flow            (.connect (FlowConnector. (merge {"cascading.flow.job.pollinginterval" 100} cascalog.rules/*JOB-CONF*))
                          "" sourcemap sinkmap trapmap (into-array Pipe tails))]
        (.complete flow)))

(defmacro ?<-
  "Helper that both defines and executes a query in a single call."
  [output & body]
  `(?- ~output (<- ~@body)))

(defn union
  "Merge the tuples from the subqueries together into a single subquery and ensure uniqueness of tuples."
  [& gens]
  (cascalog.rules/combine* gens true))

(defn combine
  "Merge the tuples from the subqueries together into a single subquery. Doesn't ensure uniqueness of tuples."
  [& gens]
  (cascalog.rules/combine* gens false))


;; Defining custom operations

(defalias defmapop w/defmapop)

(defalias defmapcatop w/defmapcatop)

(defalias defbufferop w/defbufferop)

(defalias defbufferiterop w/defbufferiterop)

(defalias defaggregateop w/defaggregateop)

(defalias deffilterop w/deffilterop)

(defalias defparallelagg p/defparallelagg)

(defalias defparallelbuf p/defparallelbuf)


;; Knobs for Hadoop

(defmacro with-job-conf
  "Modifies the job conf for queries executed within the form. Nested with-job-conf calls 
   will merge configuration maps together, with innermost calls taking precedence on conflicting
   keys."
  [conf & body]
  `(binding [cascalog.rules/*JOB-CONF* (merge cascalog.rules/*JOB-CONF* ~conf)]
    ~@body ))


;; Functions for creating taps and tap helpers

(defn select-tap-fields
  "Create a subquery that selects {fields} from {tap} and emits them in the order given."
  [tap fields]
  (let [pname (uuid)
        outfields (gen-nullable-vars (count fields))
        pipe (w/assemble (w/pipe pname) (w/identity fields :fn> outfields :> outfields))]
    (p/predicate p/generator true {pname tap} pipe outfields {})))

(defn hfs-textline
  "Creates a tap on HDFS using textline format. Different filesystems can 
   be selected by using different prefixes for {path}.
   
   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/TextLine.html"
  [path]
  (w/hfs-tap (w/text-line ["line"] Fields/ALL) path))

(defn lfs-textline
    "Creates a tap on the local filesystem using textline format.
   
   See http://www.cascading.org/javadoc/cascading/tap/Lfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/TextLine.html"
  [path]
  (w/lfs-tap (w/text-line ["line"] Fields/ALL) path))

(defn hfs-seqfile
  "Creates a tap on HDFS using sequence file format. Different filesystems can 
   be selected by using different prefixes for {path}.
   
   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path]
  (w/hfs-tap (w/sequence-file Fields/ALL) path))

(defn lfs-seqfile
   "Creates a tap that reads data off of the local filesystem in sequence file format.
   
   See http://www.cascading.org/javadoc/cascading/tap/Lfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path]
  (w/lfs-tap (w/sequence-file Fields/ALL) path))

(defn stdout
  "Creates a tap that prints tuples sunk to it to standard output. Useful for 
   experimentation in the REPL."
  [] (StdoutTap.))


;; Miscellaneous helpers

(defn div
  "Perform floating point division on the arguments. Use this instead of / in Cascalog queries since / produces
   Ratio types which aren't serializable by Hadoop."
  [f & rest] (apply / (double f) rest))

(defmacro with-debug
  "Wrap queries in this macro to cause debug information for the query planner to be printed out."
  [& body]
  `(binding [cascalog.debug/*DEBUG* true]
    ~@body ))
