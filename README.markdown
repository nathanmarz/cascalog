# About

Cascalog is a fully-featured data processing and querying library for Clojure. The main use cases for Cascalog are processing "Big Data" on top of Hadoop or doing analysis on your local computer from the Clojure REPL. Cascalog is a replacement for tools like Pig, Hive, and Cascading.

Cascalog operates at a significantly higher level of abstraction than a tool like SQL. More importantly, its tight integration with Clojure gives you the power to use abstraction and composition techniques with your data processing code just like you would with any other code. It's this latter point that sets Cascalog far above any other tool in terms of expressive power.

Follow the getting started steps, check out the tutorial, and you'll be running Cascalog queries on your local computer within 5 minutes.

# Getting Started

The best way to get started with Cascalog is experiment with the toy datasets that ship with the project. These datasets are served from memory and can be played with purely from the REPL. Just follow these steps and you'll be on your way:

1. Install [leiningen](http://github.com/technomancy/leiningen)
2. Make sure you have Java 1.6
3. export JAVA_OPTS=-Xmx768m
4. checkout the Cascalog project using Git
5. lein deps && lein compile && lein repl
6. Work through the examples in the [introductory](http://nathanmarz.com/blog/introducing-cascalog-a-clojure-based-query-language-for-hado.html) [tutorials](http://nathanmarz.com/blog/new-cascalog-features-outer-joins-combiners-sorting-and-more.html)

(Note that Cascalog is compatible with Clojure 1.2.0, 1.2.1 and 1.3.0.)

# Documentation and Issue Tracker

Documentation and issue tracker can be found on the [Cascalog wiki](https://www.assembla.com/wiki/show/d9Z8_q-Omr35zteJe5cbLr)

# Questions

Google group: [cascalog-user](http://groups.google.com/group/cascalog-user)

Come chat in the #cascalog or #cascading rooms on freenode

# Using Cascalog within a project

Cascalog is hosted at [Clojars](http://clojars.org/cascalog). Clojars is a maven repo that is trivially easy to use with maven or leiningen.


# Priorities for Cascalog development

1. Replicated and bloom joins
2. Cross query optimization: push constants and filters down into subqueries when possible


# Acknowledgements

YourKit is kindly supporting open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and
[YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).


Cascalog is based off of a very early branch of cascading-clojure project (http://github.com/clj-sys/cascading-clojure). Special thanks to Bradford Cross and Mark McGranaghan for their work on that project. Much of that code appears within Cascalog in either its original form or a modified form.
