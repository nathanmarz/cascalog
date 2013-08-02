(defproject cascalog/cascalog-math "1.10.2"
  :description "Math modules for Cascalog."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :provided {:dependencies [[cascalog/cascalog-core "1.10.2"]]}
             :dev {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]
                                  [cascalog/midje-cascalog "1.10.2"]]}})
