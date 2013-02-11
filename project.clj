(defproject cascalog "1.10.1-SNAPSHOT"
  :description "Hadoop without the Hassle."
  :url "http://www.cascalog.org"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :mailing-list {:name "Cascalog user mailing list"
                 :archive "https://groups.google.com/d/forum/cascalog-user"
                 :post "cascalog-user@googlegroups.com"}
  :dependencies [[cascalog/cascalog-core "1.10.1-SNAPSHOT"]
                 [cascalog/cascalog-checkpoint "1.10.1-SNAPSHOT"]]
  :plugins [[lein-sub "0.2.1"]
            [codox "0.6.4"]]
  :sub ["cascalog-core"
        "cascalog-checkpoint"]
  :codox {:src-dir-uri "http://github.com/nathanmarz/cascalog/blob/master"
          :src-linenum-anchor-prefix "L"
          :sources ["cascalog-core/src"
                    "cascalog-checkpoint/src"]})
