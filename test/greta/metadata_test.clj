(ns greta.metadata-test
  (:require [clojure.test :refer :all]
            [greta.metadata :refer :all]
            [manifold.stream :as s]))

(deftest client-test
  (let [cid 1
        msg {:api-key :metadata
             :api-version 0
             :correlation-id cid
             :client-id "greta"
             :topics []}]

    (with-open [c @(client "localhost" 9092)]
      (is @(s/put! c msg))
      (is (= cid (:correlation-id
                  @(s/take! c)))))))

(deftest topic-parition-leader-test

  (is (= {:node-id 0
          :host "localhost.localdomain"
          :port 9092}

         @(topic-partition-leader "localhost" 9092 "greta-tests" 0)))

  ;; If auto.create.topics.enable = true, this will fail with a
  ;; different error message (leader not available) the first time,
  ;; then fail again because the topic would now exist!
  (testing "when the topic doesn't exist"
    (is (= "ERROR: Kafka error: unknown-topic-or-partition"
           @(topic-partition-leader "localhost" 9092 "none-such" 0))))

  (testing "when the parition doesn't exist"
    (is (= "ERROR: Parition not found"
           @(topic-partition-leader "localhost" 9092 "greta-tests" 45)))))