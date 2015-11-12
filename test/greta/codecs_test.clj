(ns greta.codecs-test
  (:require [greta.codecs :refer :all]
            [greta.core :refer [str->bytes]]
            [clojure.test :refer :all]
            [gloss.io :as io]))


(defn round-trip?
  ([e m] (round-trip? e e m))
  ([e d m]
   (= m (->> m
             (io/encode e)
             (io/decode d)))))


(deftest produce-request-test
  (let [m {:magic-byte :zero
           :attributes :none
           :key (str->bytes "hello")
           :value (str->bytes "there!")}

        ms [{:offset 1
             :message m}]

        p {:api-version 0
           :correlation-id 1
           :client-id "greta-test"
           :required-acks 1
           :timeout 1000
           :produce [{:topic "greta-tests"
                       :messages [{:partition 0
                                   :message-set ms}]}]}

        r (conj {:api-key :produce} p)]

    (is (round-trip? message m))
    (is (round-trip? message-set ms))
    (is (round-trip? produce-request p))
    ;(is (round-trip? request r p))

    ;; Brittle? Yes. This value is spat out by the kafka log given the
    ;; payload from above (computed crc = 2447778493)
    (testing "CRC calculation given Kafka computed value."
      (let [expected  2447778493]
        (is (= expected (message-body-crc m)))))))

(deftest produce-response-test
  (let [p [{:parition 0
            :offset 123
            :error-code 0}]

        m {:correlation-id 1
           :produce [{:topic "greta-tests"
                      :results p}]}]

    (is (round-trip? partition-results p))
    (is (round-trip? produce-response m))))
