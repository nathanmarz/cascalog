(def version "2.0.0-SNAPSHOT")

(defproject cascalog ~version
  :description "Hadoop without the Hassle."
  :url "http://www.cascalog.org"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :mailing-list {:name "Cascalog user mailing list"
                 :archive "https://groups.google.com/d/forum/cascalog-user"
                 :post "cascalog-user@googlegroups.com"}
  :dependencies [[cascalog/cascalog-core ~version]
                 [cascalog/cascalog-checkpoint ~version]
                 [cascalog/cascalog-more-taps ~version]
                 [cascalog/cascalog-math ~version]
                 [cascalog/midje-cascalog ~version]]
  :plugins [[lein-sub "0.2.1"]
            [codox "0.6.4"]]
  :sub ["cascalog-core"
        "cascalog-checkpoint"
        "cascalog-more-taps"
        "cascalog-math"
        "midje-cascalog"]
  :codox {:src-dir-uri "http://github.com/nathanmarz/cascalog/blob/master"
          :src-linenum-anchor-prefix "L"
          :sources ["cascalog-core/src"
                    "cascalog-checkpoint/src"
                    "cascalog-more-taps/src"
                    "cascalog-math/src"
                    "midje-cascalog/src"]})
