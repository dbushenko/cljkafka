(ns cljkafka.core
  (:require [clojurewerkz.propertied.properties :refer [map->properties]])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer]
                 [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

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
                                                                       "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                                                                     })



(defmacro with-producer [producer properties & code]
          `(let [~producer (new KafkaProducer (map->properties (merge *producer-properties* ~properties)))]
                ~@code
                (.close ~producer))
)

(defmacro with-consumer [consumer properties topics & code]
          `(let [~consumer (new KafkaConsumer (map->properties (merge *consumer-properties* ~properties)))
                   ]
                (.subscribe ~consumer ~topics)
                ~@code
                (.close ~consumer))
)

(defn send! [producer topic message]
      (.send producer (new ProducerRecord topic message))
)

(defn poll
   ([consumer topic]
       (poll consumer topic 1000))
   ([consumer topic timeout]
       (let [polled (.poll consumer timeout)
              rs (seq (.records polled topic))]
              (doall (map #(.value %) rs)))))

(defn poll-loop
  ([consumer topic callback]
    (poll-loop consumer topic callback 1000))
  ([consumer topic callback timeout]
      (while true
             (let [ polled (.poll consumer timeout)
                      rs (seq (.records polled topic))
                    ]
                    (dorun (map #(callback (.value %)) rs))
      )))
  )
