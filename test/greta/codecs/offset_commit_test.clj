(ns greta.codecs.offset-commit-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.core :as c]
            [greta.codecs.core-test :refer [round-trip?]]
            [greta.codecs.offset-commit :refer :all]))


(deftest request-test
  (let [r {:api-key :offset-commit
           :api-version 0
           :correlation-id 1
           :client-id "greta-test"
           :consumer-group-id "my-group"
           :consumer-group-generation-id 10
           :consumer-id "my-consumer"
           :retention-time 10000
           :topics [{:topic "greta-tests"
                     :partitions [{:partition-id 0
                                  :offset 101
                                  :metadata "funky"}]}]}]

    (is (round-trip? request r))))


(deftest response-test
  (let [r {:correlation-id 1
           :topics [{:topic "greta-tests"
                     :partitions [{:partition-id 1
                                   :error-code :none}]}]}]

    (is (round-trip? response r))))
