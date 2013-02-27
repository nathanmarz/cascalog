(defproject cascalog/cascalog-lzo "1.10.1-SNAPSHOT"
  :description "Lzo compression taps for Cascalog."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :dependencies [[cascalog/cascalog-core "1.10.1-SNAPSHOT"]
                 [com.twitter.elephantbird/elephant-bird-cascading2 "3.0.7"]
                 [hadoop-lzo "0.4.15"]]
  :plugins [[lein-midje "3.0-beta1"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0-RC1"]]}
             :dev {:dependencies
                   [[org.apache.hadoop/hadoop-core "1.0.3"]    
                    [org.apache.httpcomponents/httpclient "4.2.3"]                
                    [cascalog/midje-cascalog "1.10.1-SNAPSHOT"
                     :exclusions [org.clojure/clojure]]]}})