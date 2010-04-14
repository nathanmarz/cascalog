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

# TODO

1. Make queries that use only aggregators that are associative & commutative lightning fast (count, sum, etc.)
2. Make predicates such as (like ?p ?p) work.
3. "Logical outer joins"

# Limitations

1. Predicates such as (likes ?p ?p) don't work yet, this will be fixed shortly
2. Query planner doesn't optimize across subqeries yet
3. No way to order your results yet
4. No recursion - I want to see concrete use cases people have for recursion first
5. No negation


# Acknowledgements

Cascalog is based off of a very early branch of cascading-clojure project (http://github.com/clj-sys/cascading-clojure). Special thanks to Bradford Cross and Mark McGranaghan for their work on that project. Much of that code appears within Cascalog in either its original form or a modified form.
