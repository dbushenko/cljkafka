(ns cljkafka.core
  (:require [clojurewerkz.propertied.properties :refer [map->properties]]
            [clojure.string :as s]
            [taoensso.timbre :as log])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.errors WakeupException]
           [java.net URI]
           [com.github.jkutner EnvKeyStore]))

;; Default properties
;;

(def ^:dynamic *producer-properties* { "bootstrap.servers" "localhost:9092"
                                       "acks" "all"
                                       "retries" "0"
                                       "batch.size" "16384"
                                       "auto.commit.interval.ms" "1000"
                                       "linger.ms" "0"
                                       "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                                       "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                                       "block.on.buffer.full" "true"})

(def ^:dynamic *consumer-properties* { "bootstrap.servers" "localhost:9092"
                                       "group.id" "test"
                                       "enable.auto.commit" "true"
                                       "auto.commit.interval.ms" "1000"
                                       "session.timeout.ms" "30000"
                                       "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                                       "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})

;; Protocols
;;

(defprotocol Close
    "Protocol for closeable object"
    (close [ this ] "Closes object")
)

(defprotocol Produce
    "Protocol for Kafka producers"
    (send! [ this topic message ] "Sends message to topic")
    (flush! [ this ] "Flushes the buffers")
)

(defprotocol Consume
    "Protocol for Kafka consumers"
    (subscribe [ this topic ] "Subscribes to one topic")
    (consume-loop [ this topic callback ] "Consumes messages infinitely")
    (stop-consume-loop [ this ])
)


;; Producer
;;
;; Totally thread safe and asynchronous, share it between multiple threads.

(defrecord Producer [ producer properties ]
    Close    
    (close [ _ ]
        (.close producer)
    )

    Produce
    (send! [ _ topic message ]
        (.send producer (new ProducerRecord topic message))
    )

    (flush! [ _ ]
        (.flush producer)
    )
)

(defn create-producer
    ([] (create-producer {}))
    ([ properties ]
        (map->Producer { :producer (new KafkaProducer (map->properties (merge *producer-properties* properties)))
                       , :properties properties}))
)

;; Uses raw java properties
(defn create-producer-with-props [ properties ]
    (map->Producer { :producer (new KafkaProducer properties)
                     , :properties properties})
)

;; Consumer
;;
;; NOT thread safe! Create new consumer for each thread!
;; Running consume-loop on one thread you make stop from the
;; other thread with function stop-consume-loop.
;; More on multithreading read here:
;; https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html

(defrecord Consumer [ consumer properties timeout consuming ]
    Close
    (close [ _ ]
        (.close consumer)
    )

    Consume
    (subscribe [ _ topic ]
        (.subscribe consumer [ topic ])
    )
    
    (consume-loop [ _ topic callback ]
        (reset! consuming true)
        (try
            (while @consuming
                (let [ polled (.poll consumer timeout)
                       rs (seq (.records polled topic))
                     ]
                     (dorun (map #(callback (.value %)) rs))))
            (catch WakeupException e))
    )

    (stop-consume-loop [ _ ]
        (reset! consuming false)
        (.wakeup consumer)
    )
)

(defn create-consumer
    ([] (create-consumer {} 1000))
    ([ properties timeout ]
        (map->Consumer {:consumer (new KafkaConsumer (map->properties (merge *consumer-properties* properties)))
                        :properties properties
                        :timeout timeout
                        :consuming (atom false)}))
)

;; for heroku

(defn- producer-properties [ url ]
    (try
        (let [uri (URI. url)
              host (.getHost uri)
              port (.getPort uri)
              envTrustStore (EnvKeyStore/createWithRandomPassword "KAFKA_TRUSTED_CERT")
              envKeyStore (EnvKeyStore/createWithRandomPassword "KAFKA_CLIENT_CERT_KEY" "KAFKA_CLIENT_CERT")
              trustStore (.storeTemp envTrustStore)
              keyStore (.storeTemp envKeyStore)
              ]
              {"security.protocol" (if (.equals (.getScheme uri) "kafka") "PLAINTEXT" "SSL")
               "ssl.truststore.type" (.type envTrustStore)
               "ssl.truststore.location" (.getAbsolutePath trustStore)
               "ssl.truststore.password" (.password envTrustStore)
               "ssl.keystore.type" (.type envKeyStore)
               "ssl.keystore.location" (.getAbsolutePath keyStore)
               "ssl.keystore.password" (.password envKeyStore)
               "bootstrap.servers" (str host ":" port)
               "acks" "all"
               "retries" "0"
               "batch.size" "16384"
               "linger.ms" "1"
               "buffer.memory" "33554432"
               "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
               "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
              }
        )
        (catch Exception e))
)

(defn heroku-producers []
    (when-not (nil? (System/getenv "KAFKA_URL"))
        (filter (comp not nil?)
                (map producer-properties
                     (s/split (System/getenv "KAFKA_URL") #","))))
)



(defn- consumer-properties [ url ]
    (try
        (let [uri (URI. url)
              host (.getHost uri)
              port (.getPort uri)
              envTrustStore (EnvKeyStore/createWithRandomPassword "KAFKA_TRUSTED_CERT")
              envKeyStore (EnvKeyStore/createWithRandomPassword "KAFKA_CLIENT_CERT_KEY" "KAFKA_CLIENT_CERT")
              trustStore (.storeTemp envTrustStore)
              keyStore (.storeTemp envKeyStore)
              ]
              {"security.protocol" (if (.equals (.getScheme uri) "kafka") "PLAINTEXT" "SSL")
               "ssl.truststore.type" (.type envTrustStore)
               "ssl.truststore.location" (.getAbsolutePath trustStore)
               "ssl.truststore.password" (.password envTrustStore)
               "ssl.keystore.type" (.type envKeyStore)
               "ssl.keystore.location" (.getAbsolutePath keyStore)
               "ssl.keystore.password" (.password envKeyStore)
               "bootstrap.servers" (str host ":" port)
               "group.id" "test"
               "enable.auto.commit" "true"
               "auto.commit.interval.ms" "1000"
               "session.timeout.ms" "30000"
               "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
               "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"              }
        )
        (catch Exception e))
)

(defn heroku-consumers []
    (when-not (nil? (System/getenv "KAFKA_URL"))
        (filter (comp not nil?)
                (map consumer-properties
                     (s/split (System/getenv "KAFKA_URL") #","))))
)
