(ns greta.codecs.offset-fetch-test
  (:require [clojure.test :refer :all]
            [greta.codecs.core-test :refer [round-trip?]]
            [greta.codecs.offset-fetch :refer :all]))


(deftest request-test
  (let [r {:api-key :offset-fetch
           :api-version 0
           :correlation-id 1
           :client-id "greta-test"
           :consumer-group "my-group"
           :topics [{:topic "greta-tests"
                     :partitions [0]}]}]

    (is (round-trip? request r))))

(deftest response-test
  (let [r {:correlation-id 1
           :topics [{:topic "greta-tests"
                     :partitions [{:partition-id 0
                                   :offset 101
                                   :metadata "funky-town"
                                   :error-code :none}]}]}]

    (is (round-trip? response r))))
