Cascalog allows you to query Hadoop in Clojure with an expressive language inspired by Datalog.

Cascalog also features a wrapper around Cascading to define dataflows. Custom operations defined in Cascalog can be used both for Cascalog queries and Cascalog dataflows.


# Getting started

1. Make sure you have java 1.6
2. Set JAVA_OPTS environment variable to 768m (export JAVA_OPTS=-Xmx768m)
3. install leiningen (http://github.com/technomancy/leiningen)
4. lein deps
5. lein compile-java && lein compile
6. optionally run lein test to make sure tests pass

# Tutorial

See http://nathanmarz.com/blog/introducing-cascalog for a tutorial


# Experiment

1. Launch a repl with "lein repl"
2. (use 'cascalog.playground) (bootstrap) to play with test datasets defined in playground.clj.


# Limitations

- talk about limitiations in README... things like (likes ?p ?p) don't work yet, query planner doesn't optimize across rules yet, working on providing a way to order your results, no recursion yet, no negation yet


# Acknowledgements

Cascalog is based off of a very early branch of cascading-clojure project (http://github.com/clj-sys/cascading-clojure). Special thanks to Bradford Cross and Mark McGranaghan for their work on that project, particularly with the code that allows Clojure functions to be used within Cascading flows.
