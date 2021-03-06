(defproject cljkafka "0.3.14-SNAPSHOT"
  :description "Simple kafka wrapper"
  :url "https://github.com/dbushenko/cljkafka"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
            :dependencies [ [org.clojure/clojure "1.8.0"]
                            [org.apache.kafka/kafka-clients "0.10.0.0"]
                            [com.github.jkutner/env-keystore "0.1.2"]
                            [clojurewerkz/propertied "1.2.0"]
                          ])
