# cljkafka

A simple Clojure library designed to help using Kafka.

The library is very simple yet customizable enough.

## Usage

    (require '[cljkafka.core :refer :all])

    (def p (create-producer))
    (send! p "test" "hello from clojure!")

    (def c (create-consumer))
    (subscribe c "test")
    (consume-loop c "test" println)

The library implements a common pattern of consuming messages using loop. More on this see [https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html](here). 

Copyright © 2016 by Dmitry Bushenko

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
