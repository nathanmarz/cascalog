(defproject cascalog "1.0.0-SNAPSHOT"
  :source-path "src/clj"
  :library-path "_deps"
  :java-source-path "src/jvm"
  :java-fork "true"
  :javac-debug "true"
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0"]]
  :dev-dependencies [[lein-javac "0.0.2-SNAPSHOT"]]
  :namespaces [cascalog.workflow
               cascalog.core
               cascalog.util
               cascalog.vars
               cascalog.predicate
               cascalog.workflow-example
               cascalog.testing
               cascalog.graph])
