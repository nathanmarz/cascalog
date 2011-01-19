(defproject cascalog/cascalog "1.6.0"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [cascading/cascading-core "1.2-wip-63" :exclusions [org.codehaus.janino/janino]]
                 [log4j/log4j "1.2.16"]
                 ]
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dev-dependencies [
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.2.1"]
                    ]
  :aot :all)
