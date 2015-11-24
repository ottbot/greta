(ns greta.core-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.core :as c]
            [greta.codecs.fetch :as fetch-codecs]
            [greta.codecs.metadata :as metadata-codecs]
            [greta.codecs.produce :as produce-codecs]
            [greta.core :refer :all]
            [manifold.stream :as s]))


(deftest metadata-test
  (let [cid 1
        msg {:api-key :metadata
             :api-version 0
             :correlation-id cid
             :client-id "greta"
             :topics []}]

    (with-open [c @(client "localhost" 9092
                          metadata-codecs/request
                          metadata-codecs/response)]
      (is @(s/put! c msg))
      (is (= cid (:correlation-id
                  @(s/take! c)))))))


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
