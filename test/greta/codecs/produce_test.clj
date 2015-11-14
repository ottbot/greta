(ns greta.codecs.produce-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.produce :refer :all]
            [greta.codecs.core-test :refer [round-trip?]]
            [greta.core :refer [str->bytes]]))


(deftest produce-request-test
  (let [m {:magic-byte :zero
           :attributes :none
           :key (str->bytes "hello")
           :value (str->bytes "there!")}

        ms [{:offset 1
             :message m}]

        r {:api-key :produce
           :api-version 0
           :correlation-id 1
           :client-id "greta-test"
           :required-acks 1
           :timeout 1000
           :produce [{:topic "greta-tests"
                       :messages [{:partition 0
                                   :message-set ms}]}]}]

    (is (round-trip? message m))
    (is (round-trip? message-set ms))
    (is (round-trip? request r))


    ;; Brittle? Yes. This value is spat out by the kafka log given the
    ;; payload from above (computed crc = 2447778493)
    (testing "CRC calculation given Kafka computed value."
      (let [expected  2447778493]
        (is (= expected (message-body-crc m)))))))

(deftest produce-response-test
  (let [m {:correlation-id 1
           :produce [{:topic "greta-tests"
                      :results [{:parition 0
                                 :offset 123
                                 :error-code 0}]}]}]

    (is (round-trip? response m))))
