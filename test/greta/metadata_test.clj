(ns greta.metadata-test
  (:require [clojure.test :refer :all]
            [greta.metadata :refer :all]
            [greta.test-utils :as utils]
            [manifold.stream :as s]))


(deftest topic-partition-leader-test

  (is (every? @(topic-partition-leader utils/kafka-host
                                       utils/kafka-port
                                       "greta-tests" 0)
              [:host :port :node-id]))

  ;; If auto.create.topics.enable = true, this will fail with a
  ;; different error message (leader not available) the first time,
  ;; then fail again because the topic would now exist!
  (testing "when the topic doesn't exist"
    (is (thrown-with-msg?
         Exception
         #"Kafka error"
         @(topic-partition-leader utils/kafka-host
                                  utils/kafka-port
                                  "none-such" 0))))

  (testing "when the partition doesn't exist"
    (is (thrown-with-msg?
         Exception
         #"Partition not found"
         @(topic-partition-leader utils/kafka-host
                                  utils/kafka-port
                                  "greta-tests" 45)))))
