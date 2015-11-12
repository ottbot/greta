(ns greta.codecs.metadata-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.metadata :refer :all]
            [greta.codecs-test :refer [round-trip?]]))


(deftest metadata-request-test
  (let [r {:api-key :metadata
           :api-version 0
           :correlation-id 1
           :client-id "greta-test"
           :topics ["my" "nice" "topics"]}]

    (is (round-trip? request r))))

(deftest metadata-response-test
  (let [b [{:node-id 1
             :host "example.com"
             :port 9092}]

        p [{:partition-error-code 0
            :partition-id 1
            :leader 1
            :replicas [1]
            :isr [1]}]

        t [{:topic-error-code 0
            :topic-name "greata-tests"
            :parition-metadata p}]

        m {:correlation-id 1
           :brokers b
           :topics t}]

    (is (round-trip? brokers b))
    (is (round-trip? partition-metadata p))
    (is (round-trip? topics t))
    (is (round-trip? response m))))
