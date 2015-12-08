(ns greta.codecs-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs :refer :all]
            [greta.serde :as serde]))

(defn round-trip?
  ([i m] (round-trip? i i m))
  ([i o m]
   (= m (->> m
             (io/encode i)
             (io/decode o)))))


(deftest metadata-test
  (let [api (metadata)

        req {:topics ["my" "nice" "topics"]}

        res {:brokers [{:node-id 1
                        :host "example.com"
                        :port 9092}]

             :topics [{:topic-error-code :none
                       :topic-name "greata-tests"
                       :partition-metadata [{:partition-error-code :none
                                             :partition-id 1
                                             :leader 1
                                             :replicas [1]
                                             :isr [1]}]}]}]

    (is (round-trip? (request api) req))
    (is (round-trip? (response api) res))))


(deftest offset-test
  (let [api (offset)

        req {:replica-id 1
             :topics [{:topic "greta-tests"
                       :partitions [{:partition 0
                                     :time 100
                                     :max-number-of-offsets 100}]}]}

        res [{:topic "greta-tests"
              :partitions [{:partition 0
                            :error-code :none
                            :offsets [0]}]}]]

    (is (round-trip? (request api) req))
    (is (round-trip? (response api) res))))


(deftest offset-fetch-test
  (let [api (offset-fetch)

        req {:consumer-group "my-group"
             :topics [{:topic "greta-tests"
                       :partitions [0]}]}

        res [{:topic "greta-tests"
              :partitions [{:partition-id 0
                            :offset 101
                            :metadata "funky-town"
                            :error-code :none}]}]]
    (is (round-trip? (request api) req))
    (is (round-trip? (response api) res))))





(deftest offset-commit-test
  (let [api (offset-commit)

        req {:consumer-group-id "my-group"
             :consumer-group-generation-id 10
             :consumer-id "my-consumer"
             :retention-time 10000
             :topics [{:topic "greta-tests"
                       :partitions [{:partition-id 0
                                     :offset 101
                                     :metadata "funky"}]}]}

        res [{:topic "greta-tests"
              :partitions [{:partition-id 1
                            :error-code :none}]}]]

    (is (round-trip? (request api) req))
    (is (round-trip? (response api) res))))


(deftest fetch-test
  (let [api (fetch (serde/string-serde))

        req {:replica-id 1
             :max-wait-time 1000
             :min-bytes 64
             :topics [{:topic "greta-tests"
                       :messages [{:partition 0
                                   :fetch-offset 0
                                   :max-bytes 640}]}]}


        res [{:topic-name "greta-tests"
               :messages [{:partition 1
                           :error-code :none
                           :highwater-mark-offset 0
                           :message-set [{:offset 1
                                          :message {:magic-byte :zero
                                                    :attributes :none
                                                    :key "hello"
                                                    :value "there!"}}]}]}]]


    (is (round-trip? (request api) req))
    (is (round-trip? (response api) res))))





(deftest produce-test
  (let [api (produce (serde/string-serde))

        req {:required-acks 1
             :timeout 1000
             :produce [{:topic "greta-tests"
                        :messages [{:partition 0
                                    :message-set [{:offset 1
                                                   :message {:magic-byte :zero
                                                             :attributes :none
                                                             :key "hello"
                                                             :value "there!"}}]}]}]}
        res [{:topic "greta-tests"
              :results [{:partition 0
                         :offset 123
                         :error-code :none}]}]]

    (is (round-trip? (request api) req))
    (is (round-trip? (response api) res))))
