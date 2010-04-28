Cascalog allows you to query Hadoop in Clojure with an expressive language inspired by Datalog. Follow the getting started steps, check out the tutorial, and you'll be running Cascalog queries locally within 5 minutes.

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


# Running Cascalog queries on a Hadoop cluster

1. Cascalog includes hadoop as a dependency so that you can experiment with it easily. Don't include Hadoop jars within your jar that has Cascalog.
2. Cascalog requires Cascading 1.1
3. Any custom operations must be compiled into the jar you give to Hadoop for running jobs

# Questions or Concerns?

Send an email to nathan.marz@gmail.com, open on issue on Github, or come chat in the #cascading room on freenode

# Query planner highlights

1. Automatically drops fields when they're no longer needed to minimize amount of data pushed around
2. Auto-combiners - operations like min, max, count, sum are aggregated map-side in preparation for actual aggregation when possible
3. Filters data as early as possible within a query definition

# TODO

1. Ungrounded rules
2. Combine join with aggregation using MultiGroupBy when grouping fields and join fields are the same

# Limitations

1. Query planner doesn't optimize across subqeries yet
2. No recursion
3. No negation


# Acknowledgements

Cascalog is based off of a very early branch of cascading-clojure project (http://github.com/clj-sys/cascading-clojure). Special thanks to Bradford Cross and Mark McGranaghan for their work on that project. Much of that code appears within Cascalog in either its original form or a modified form.
