(ns greta.core-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.core :as c]
            [greta.codecs.fetch :as fetch-codecs]
            [greta.codecs.offset :as offset-codecs]
            [greta.codecs.produce :as produce-codecs]
            [greta.core :refer :all]
            [manifold.stream :as s]))


(deftest produce-test
  (let [cid 2

        ms {:partition 0
            :message-set [{:offset 0
                           :message
                           {:magic-byte :zero
                            :attributes :none
                            :key ""
                            :value "see you on the other side!"}}]}

        msg {:api-key :produce
             :api-version 0
             :correlation-id cid
             :client-id "greta"
             :required-acks 1
             :timeout 1000
             :produce [{:topic "greta-tests"
                        :messages [ms]}]}]

    (with-open [c @(client "localhost" 9092
                           (produce-codecs/request (c/string-serde))
                           produce-codecs/response )]

      (is @(s/put! c msg))
      (is (= cid (:correlation-id
                @(s/take! c)))))))


(deftest fetch-test
  (let [cid 3
        msg {:api-key :fetch
             :api-version 0
             :correlation-id cid
             :client-id "greta-test"
             :replica-id -1
             :max-wait-time 1000
             :min-bytes 10
             :topics [{:topic "greta-tests"
                      :messages [{:partition 0
                                  :fetch-offset 0
                                  :max-bytes 10240}]}]}]

    (with-open [c @(client "localhost" 9092
                           fetch-codecs/request
                           (fetch-codecs/response (c/string-serde)))]
      (is @(s/put! c msg))
      (is (not-empty
           (get-in
            @(s/try-take! c ::drained 1000 ::timeout)
            [:topics 0 :messages]))))))


(deftest offset-test
  (let [cid 4

        msg {:api-key :offset
             :api-version 0
             :correlation-id cid
             :client-id "greta-tests"
             :replica-id -1
             :topics [{:topic "greta-tests"
                       :partitions [{:partition 0
                                     :time -2
                                     :max-number-of-offsets 100}]}]}]

    (with-open [c @(client "localhost" 9092
                           offset-codecs/request
                           offset-codecs/response)]
      (is @(s/put! c msg))
      (is (= cid
             (:correlation-id
              @(s/take! c)))))))
