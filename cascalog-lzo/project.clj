(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject cascalog/cascalog-lzo VERSION
  :description "Lzo compression taps for Cascalog."
  :plugins [[lein-modules "0.3.11"]]
  :dependencies [[com.twitter.elephantbird/elephant-bird-cascading2 "4.6"
                  :exclusions [cascading/cascading-hadoop]]
                 [com.hadoop.gplcompression/hadoop-lzo "0.4.19"]]
  :profiles {:provided {:dependencies
                        [[cascalog/cascalog-core :version]
                         [org.apache.hadoop/hadoop-common "2.4.0"]
                         [org.apache.hadoop/hadoop-mapreduce-client-jobclient "2.4.0"]
                         [org.apache.httpcomponents/httpclient "4.2.3"]]}
             :dev {:dependencies [[cascalog/midje-cascalog :version]]}})
