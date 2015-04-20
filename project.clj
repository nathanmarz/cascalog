(def VERSION (or VERSION (slurp "./VERSION")))
(def CC-VERSION (or (System/getenv "CASCALOG_CASCADING_VERSION") "2.5.3"))

(defproject cascalog/cascalog VERSION
  :description "Hadoop without the Hassle."
  :profiles {:provided
             {:dependencies [[org.clojure/clojure "_"]
                             [org.apache.hadoop/hadoop-core "_"]]}
             :dev {:plugins [[lein-midje "3.1.3"]]}
             :fast {:modules {:subprocess nil}}}
  :plugins [[lein-modules "0.3.11"]]
  :modules {:inherited
            {:repositories {"conjars" "http://conjars.org/repo/"}
             :deploy-repositories
             [["releases" {:url "https://clojars.org/repo/" :creds :gpg}]]
             :scm {:dir ".."}
             :url "http://www.cascalog.org"
             :license {:name "Eclipse Public License"
                       :url "http://www.eclipse.org/legal/epl-v10.html"}
             :mailing-list
             {:name "Cascalog user mailing list"
              :archive "https://groups.google.com/d/forum/cascalog-user"
              :post "cascalog-user@googlegroups.com"}}
            :dirs ["cascalog-core"
                   "cascalog-checkpoint"
                   "cascalog-lzo"
                   "cascalog-more-taps"
                   "cascalog-math"
                   ;; "midje-cascalog"
                   ]
            :versions
            {cascading ~CC-VERSION
             log4j "1.2.16"
             org.slf4j/slf4j-log4j12 "1.6.6"
             org.clojure/clojure "1.6.0"
             org.apache.hadoop/hadoop-core "1.2.1"
             prismatic/schema "0.3.7"
             hadoop-util "0.3.0"
             jackknife "0.1.7"}})
