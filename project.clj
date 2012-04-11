(def shared
  '[[org.clojure/tools.macro "0.1.1"]
    [cascading/cascading-hadoop "2.0.0-wip-226"
     :exclusions [org.codehaus.janino/janino
                  org.apache.hadoop/hadoop-core]]
    [org.clojure/tools.macro "0.1.1"]
    [cascading.kryo "0.2.1"]
    [cascalog/carbonite "1.1.1"]
    [log4j/log4j "1.2.16"]
    [hadoop-util "0.2.7"]
    [jackknife "0.1.2"]])

(defproject cascalog/cascalog "1.9.0-wip8"
  :description "Hadoop without the Hassle."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.0.0"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :jvm-opts ["-Xmx768m" "-server"
             "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo/"}
  :codox {:include [cascalog.vars cascalog.ops cascalog.io cascalog.api]}
  :plugins [[lein-midje "2.0.0-SNAPSHOT"]]
  :aliases { "all" ["with-profile" "dev:1.2,dev:1.4"]}
  :dependencies ~(conj shared '[org.clojure/clojure "1.3.0"])
  :profiles {:all {:dependencies ~shared}
             :1.2 {:dependencies [[org.clojure/clojure "1.2.1"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0-alpha3"]]}
             :dev {:dependencies
                   [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                    [midje-cascalog "0.4.0" :exclusions [org.clojure/clojure]]]}})
