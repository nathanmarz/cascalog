Cascalog allows you to query Hadoop in Clojure with an expressive language inspired by Datalog. Follow the getting started steps, check out the tutorial, and you'll be running Cascalog queries on your local computer within 5 minutes.

Cascalog also features a wrapper around Cascading to define dataflows in cascalog.workflow . Custom operations defined in Cascalog can be used both for Cascalog queries and Cascalog dataflows.

# Getting started

1. Make sure you have java 1.6
2. export JAVA_OPTS=-Xmx768m
3. install [leiningen](http://github.com/technomancy/leiningen)
4. git clone git://github.com/nathanmarz/cascalog.git
5. cd cascalog && lein deps && lein compile-java && lein compile
6. optionally run "lein test" to make sure tests pass

# Tutorials

1. [Introducing Cascalog](http://nathanmarz.com/blog/introducing-cascalog)
2. [New Cascalog features: outer joins, combiners, sorting, and more](http://nathanmarz.com/blog/new-cascalog-features/)
3. [News Feed in 38 lines of code using Cascalog](http://nathanmarz.com/blog/cascalog-news-feed)
4. [Cascalog features for consuming wide taps](http://groups.google.com/group/cascalog-user/browse_thread/thread/17abcbed12d76232)
5. [Predicate macros](http://groups.google.com/group/cascalog-user/browse_thread/thread/33f9b69bf18c9bdc)

# Running Cascalog queries on a Hadoop cluster

1. Cascalog includes hadoop as a dependency so that you can experiment with it easily. Don't include Hadoop jars within your jar that has Cascalog.
2. Cascalog requires Cascading 1.1
3. Any custom operations must be compiled into the jar you give to Hadoop for running jobs

# Questions or Concerns?

Google group: [cascalog-user](http://groups.google.com/group/cascalog-user)

IRC: Come chat in the #cascading room on freenode

# Priorities for Cascalog development

1. Replicated and bloom joins
2. Cross query optimization: push constants and filters down into subqueries when possible
3. Negations, i.e. "people who like dogs and don't like cats" (<- \[?p] (likes ?p "dogs") (likes ?p "cats" :> false)) [implement with multigroupby of some sort]
4. Disjunction, i.e. "all people over 30 years old and all males" (<- \[?p] \[(age ?p ?a) (> ?a 30)] \[(gender ?p "m")])])
5. Ungrounded subqueries, i.e.: (def avg (<- \[?val :> ?avg] (c/count ?c) (c/sum ?val :> ?s) (div ?s ?c :> ?avg))) 
6. Recursion, i.e. "all ancestry relations" (<- \[?a ?p] \[(parent ?a ?p)] \[(parent ?a ?p2) (recur ?p2 ?p))])


# Acknowledgements

YourKit is kindly supporting open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and
[YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).


Cascalog is based off of a very early branch of cascading-clojure project (http://github.com/clj-sys/cascading-clojure). Special thanks to Bradford Cross and Mark McGranaghan for their work on that project. Much of that code appears within Cascalog in either its original form or a modified form.
