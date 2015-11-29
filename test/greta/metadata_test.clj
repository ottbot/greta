(ns greta.metadata-test
  (:require [clojure.test :refer :all]
            [greta.metadata :refer :all]
            [manifold.stream :as s]))


(deftest topic-partition-leader-test

  (is (every? @(topic-partition-leader "localhost" 9092
                                       "greta-tests" 0)
              [:host :port :node-id]))

  ;; If auto.create.topics.enable = true, this will fail with a
  ;; different error message (leader not available) the first time,
  ;; then fail again because the topic would now exist!
  (testing "when the topic doesn't exist"
    (is (= "ERROR: Kafka error: unknown-topic-or-partition"
           @(topic-partition-leader "localhost" 9092 "none-such" 0))))

  (testing "when the partition doesn't exist"
    (is (= "ERROR: Partition not found"
           @(topic-partition-leader "localhost" 9092 "greta-tests" 45)))))
