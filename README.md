# cljkafka

A simple Clojure library designed to help using Kafka.

The library is very simple yet customizable enough.

## Usage

    (require '[cljkafka.core :refer :all])

    (with-producer p {}
                   (send! p "test" "message from clojure"))

    (with-consumer c {} ["test"]                       ;; List of subscribed topics -- ["test"]
                   (while true
                          (println (poll c "test" 100)))) ;; 100ms -- timeout

     (with-consumer c {} ["test"]
                    (poll-loop c "test" println))           ;; Check infinitely

Copyright © 2016 by Dmitry Bushenko

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
