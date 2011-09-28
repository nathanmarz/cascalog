(defproject cascalog/cascalog "1.8.2-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :jvm-opts ["-Xmx768m" "-server"]
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 ;; jgrapht exclusion works around cascading pom bug
                 ;; that causes projects dependent on cascalog to not
                 ;; be able to find jgrapht.
                 [cascading/cascading-core "1.2.4"
                  :exclusions [org.codehaus.janino/janino
                               thirdparty/jgrapht-jdk1.6
                               riffle/riffle]]
                 [thirdparty/jgrapht-jdk1.6 "0.8.1"]
                 [riffle/riffle "0.1-dev"]
                 [log4j/log4j "1.2.16"]
                 [hadoop-util "0.2.0"]]
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]
                     [clojure-source "1.2.0"]
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]]
  :aot :all)
