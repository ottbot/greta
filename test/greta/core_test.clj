(ns greta.core-test
  (:require [clojure.test :refer :all]
            [greta.core :refer :all]
            [greta.serde :as serde]
            [manifold.stream :as s]))

(deftest client-test
  (let [msg {:header {:api-key :metadata
                      :api-version 0
                      :client-id "greta"}

             :topics []}]

    (with-open [c @(client "localhost" 9092)]
      (is @(s/put! c msg))
      (is (every? @(s/take! c)
                  [:brokers :topics])))))


(deftest produce-test'
  (let [msg {:header {:api-key :produce
                      :api-version 0
                      :correlation-id 0
                      :client-id "greta-test"}

             :required-acks 1
             :timeout 1000
             :produce [{:topic "greta-tests"
                        :messages [{:partition 0
                                    :message-set [{:offset 0
                                                   :message {:magic-byte :zero
                                                             :attributes :none
                                                             :key ""
                                                             :value "see you on the other side!"}}]}]}]}]

    (with-open [c @(client "localhost" 9092 (serde/string-serde))]

      (is @(s/put! c msg))
      (is (= :none
             (get-in
              @(s/take! c)
              [0 :results 0 :error-code]))))))


(deftest fetch-test'
  (let [msg {:header {:api-key :fetch
                      :api-version 0
                      :correlation-id 1
                      :client-id "greta-test"}

             :replica-id -1
             :max-wait-time 1000
             :min-bytes 10
             :topics [{:topic "greta-tests"
                       :messages [{:partition 0
                                   :fetch-offset 0
                                   :max-bytes 10240}]}]}]

    (with-open [c @(client "localhost" 9092 (serde/string-serde))]

      (is @(s/put! c msg))
      (is (get-in
           @(s/try-take! c ::drained 1000 ::timeout)
           [:topics 0 :messages])))))


(deftest offset-test'
  (let [msg {:header {:api-key :offset
                      :api-version 0
                      :correlation-id 0
                      :client-id "greta-tests"}

             :replica-id -1
             :topics [{:topic "greta-tests"
                       :partitions [{:partition 0
                                     :time -2
                                     :max-number-of-offsets 100}]}]}]

    (with-open [c @(client "localhost" 9092)]

      (is @(s/put! c msg))
      (is (= :none
             (get-in
              @(s/take! c)
              [0 :partitions 0 :error-code]))))))
