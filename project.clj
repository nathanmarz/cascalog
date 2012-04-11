(def shared
  '[[org.clojure/tools.macro "0.1.1"]
    [cascading/cascading-core "1.2.4"
     :exclusions [org.codehaus.janino/janino
                  thirdparty/jgrapht-jdk1.6
                  riffle/riffle]]
    [thirdparty/jgrapht-jdk1.6 "0.8.1"]
    [cascading.kryo "0.3.0-SNAPSHOT"]
    [cascalog/carbonite "1.2.0"]
    [riffle/riffle "0.1-dev"]
    [log4j/log4j "1.2.16"]
    [hadoop-util "0.2.7"]
    [jackknife "0.1.2"]])

(defproject cascalog/cascalog "1.8.7-SNAPSHOT"
  :description "Hadoop without the Hassle."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :jvm-opts ["-Xmx768m" "-server"]
  :codox {:include [cascalog.vars cascalog.ops cascalog.io cascalog.api]}
  :repositories {"conjars" "http://conjars.org/repo/"}
  :plugins [[lein-midje "2.0.0-SNAPSHOT"]]
  :aliases { "all" ["with-profile" "dev:1.2,dev:1.4"]}
  :dependencies ~(conj shared '[org.clojure/clojure "1.3.0"])
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje-cascalog "0.4.0" :exclusions [org.clojure/clojure]]]
  :profiles {:all {:dependencies ~shared}
             :1.2 {:dependencies [[org.clojure/clojure "1.2.1"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0-alpha3"]]}
             :dev {:dependencies
                   [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                    [midje-cascalog "0.4.0" :exclusions [org.clojure/clojure]]]}})
