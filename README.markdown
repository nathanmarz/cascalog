Cascalog allows you to query Hadoop in Clojure with an expressive language inspired by Datalog. Follow the getting started steps, check out the tutorial, and you'll be running Cascalog queries locally within 5 minutes.

Cascalog also features a wrapper around Cascading to define dataflows in cascalog.workflow . Custom operations defined in Cascalog can be used both for Cascalog queries and Cascalog dataflows.


# Getting started

1. Make sure you have java 1.6
2. export JAVA_OPTS=-Xmx768m
3. install [leiningen](http://github.com/technomancy/leiningen)
4. lein deps
5. lein compile-java && lein compile
6. optionally run lein test to make sure tests pass

# Tutorial

See [http://nathanmarz.com/blog/introducing-cascalog](http://nathanmarz.com/blog/introducing-cascalog) for a tutorial

# Experiment

1. Launch a repl with "lein repl"
2. (use 'cascalog.playground) (bootstrap) to play with test datasets defined in playground.clj.

# Query planner highlights

1. Automatically drops fields when they're no longer needed to minimize amount of data pushed around
2. Auto-combiners - operations like min, max, count, sum are aggregated map-side in preparation for actual aggregation when possible

# TODO

1. Make predicates such as (like ?p ?p) work
2. "Logical outer joins"
3. Combine join with aggregation using MultiGroupBy grouping fields and join fields are the same

# Notes on running Cascalog queries on a real Hadoop cluster

1. Cascalog includes hadoop as a dependency so that you can experiment with it easily. Don't include Hadoop jars within your jar that has Cascalog.
2. Cascalog requires Cascading 1.1 rc3
3. Any custom operations must be compiled into the jar you give to Hadoop for running jobs

# Limitations

1. Predicates such as (likes ?p ?p) don't work yet, this will be fixed shortly
2. Query planner doesn't optimize across subqeries yet
3. No way to order your results yet
4. No recursion - I want to see concrete use cases people have for recursion first
5. No negation


# Acknowledgements

Cascalog is based off of a very early branch of cascading-clojure project (http://github.com/clj-sys/cascading-clojure). Special thanks to Bradford Cross and Mark McGranaghan for their work on that project. Much of that code appears within Cascalog in either its original form or a modified form.
