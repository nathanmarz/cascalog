(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def HADOOP-VERSION (-> ROOT-DIR (str "/../HADOOP-VERSION") slurp))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject cascalog/cascalog-lzo VERSION
  :description "Lzo compression taps for Cascalog."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo" "twttr.com" "http://maven.twttr.com/"}
  :dependencies [[com.twitter.elephantbird/elephant-bird-cascading2 "4.6"
                  :exclusions [cascading/cascading-hadoop]]
                 [com.hadoop.gplcompression/hadoop-lzo "0.4.19"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0"]]}
             :provided {:dependencies [[cascalog/cascalog-core ~VERSION]
                                       [org.apache.hadoop/hadoop-common "2.4.0"]
                                       [org.apache.hadoop/hadoop-mapreduce-client-jobclient "2.4.0"]
                                       [org.apache.httpcomponents/httpclient "4.2.3"]]}
             :dev {:dependencies [[cascalog/midje-cascalog ~VERSION]]
                   :plugins [[lein-midje "3.1.3"]]}})
