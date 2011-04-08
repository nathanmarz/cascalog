(defproject cascalog/cascalog "1.7.0"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 ;; jgrapht exclusion works around cascading pom bug that causes projects dependent on
                 ;; cascalog to not be able to find jgrapht
                 [cascading/cascading-core "1.2-wip-63" :exclusions [org.codehaus.janino/janino thirdparty/jgrapht-jdk1.6]]
                 [thirdparty/jgrapht-jdk1.6 "0.8.1"]
                 [log4j/log4j "1.2.16"]
                 ]
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dev-dependencies [
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.2.1"]
                    ]
  :aot :all)
