# About

Cascalog is a tool for processing data on Hadoop with Clojure in a concise and expressive manner. Cascalog combines two cutting edge technologies in Clojure and Hadoop and resurrects an old one in Datalog. Cascalog is high performance, flexible, and robust.

Most query languages, like SQL, Pig, and Hive, are custom languages -- and this leads to huge amounts of accidental complexity. Constructing queries dynamically by doing string manipulation is an impedance mismatch and makes usual programming techniques like abstraction and composition difficult.

Cascalog queries are first-class within Clojure and are extremely composable. Additionally, the Datalog syntax of Cascalog is simpler and more expressive than SQL-based languages.

Follow the getting started steps, check out the tutorial, and you'll be running Cascalog queries on your local computer within 5 minutes.

# Documentation

Documentation can be found on the [Cascalog wiki](https://www.assembla.com/wiki/show/d9Z8_q-Omr35zteJe5cbLr)

# Questions

Google group: [cascalog-user](http://groups.google.com/group/cascalog-user)

Come chat in the #cascading room on freenode

# Using Cascalog within a project

Cascalog is hosted at [Clojars](http://clojars.org/cascalog). Clojars is a maven repo that is trivially easy to use with maven or leiningen.


# Priorities for Cascalog development

1. Replicated and bloom joins
2. Cross query optimization: push constants and filters down into subqueries when possible
3. Negations, i.e. "people who like dogs and don't like cats" (<- \[?p] (likes ?p "dogs") (likes ?p "cats" :> false)) [implement with multigroupby of some sort]
4. Disjunction, i.e. "all people over 30 years old and all males" (<- \[?p] \[(age ?p ?a) (> ?a 30)] \[(gender ?p "m")])])
5. Recursion, i.e. "all ancestry relations" (<- \[?a ?p] \[(parent ?a ?p)] \[(parent ?a ?p2) (recur ?p2 ?p))])


# Acknowledgements

YourKit is kindly supporting open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and
[YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).


Cascalog is based off of a very early branch of cascading-clojure project (http://github.com/clj-sys/cascading-clojure). Special thanks to Bradford Cross and Mark McGranaghan for their work on that project. Much of that code appears within Cascalog in either its original form or a modified form.
