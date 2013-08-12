(def cascalog-version "2.0.0-SNAPSHOT")

(defproject cascalog/cascalog-more-taps cascalog-version
  :description "More taps for Cascalog"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :provided {:dependencies [[cascalog/cascalog-core ~cascalog-version]]}
             :dev {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]
                                  [cascalog/midje-cascalog ~cascalog-version]
                                  [org.clojure/clojure "1.5.1"]]
                   :plugins [[lein-midje "3.0.0"]
                             [codox "0.6.4"]]}}
  :codox {:src-dir-uri "http://github.com/nathanmarz/cascalog/blob/master"
          :src-linenum-anchor-prefix "L"
          :sources ["src"
                    ]})
