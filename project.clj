(def shared-deps
  '[[org.clojure/tools.macro "0.1.1"]
    [cascading/cascading-hadoop "2.0.0-wip-184"
     :exclusions [org.codehaus.janino/janino
                  org.apache.hadoop/hadoop-core]]
    [org.clojure/tools.macro "0.1.1"]
    [cascading.kryo "0.1.5"]
    [cascalog/carbonite "1.1.1"]
    [log4j/log4j "1.2.16"]
    [hadoop-util "0.2.7"]
    [jackknife "0.1.2"]])

(defproject cascalog/cascalog "1.9.0-wip3"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :jvm-opts ["-Xmx768m" "-server"
             "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo/"}
  :codox {:include [cascalog.vars cascalog.ops cascalog.io cascalog.api]}
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [lein-multi "1.1.0-SNAPSHOT"]
                     [midje-cascalog "0.4.0"]]
  :dependencies ~(conj shared-deps '[org.clojure/clojure "1.3.0"])
  :multi-deps {"1.2" ~(conj shared-deps '[org.clojure/clojure "1.2.1"])
               "1.4" ~(conj shared-deps '[org.clojure/clojure "1.4.0-alpha3"])})
