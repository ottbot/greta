(ns greta.offset-test
  (:require [clojure.test :refer :all]
            [greta.offset :refer :all]
            [manifold.stream :as s]))


(deftest coordinator-test
  (is (every? @(coordinator "localhost" 9092 "my-group")
              [:error-code :host :port :coordinator-id])))




(deftest coordinator-test
  (is (every? @(coordinator "localhost" 9092 "my-group")
              [:host :port :error-code :coordinator-id])))


(deftest offset-committer-test
  (let [r {:api-key :offset-commit
           :api-version 1
           :correlation-id 1
           :client-id "greta-test"
           :consumer-group-id "my-group"
           :consumer-group-generation-id 1
           :consumer-id "my-group"
           :retention-time -1 ;; would like to NOT set this?
           :topics [{:topic "greta-tests"
                     :partitions [{:partition-id 0
                                   :offset 101
                                   :timestamp (System/currentTimeMillis)
                                   :metadata "funky"}]}]}]

    (with-open [c @(offset-committer "localhost"
                                     9092
                                     "my-group")]
      @(s/put! c r)
      (is (= 1
             @(s/try-take! c 1000))))))


(deftest offset-fetcher-test
  (let [r {:api-key :offset-fetch
           :api-version 1
           :correlation-id 1
           :client-id "greta-test"
           :consumer-group "my-group"
           :topics [{:topic "greta-tests"
                     :partitions [0]}]}]

    (with-open [c @(offset-fetcher "localhost"
                                   9092
                                   "my-group")]

      @(s/put! c r)
      (is (= 1 @(s/try-take! c 1000))))))
