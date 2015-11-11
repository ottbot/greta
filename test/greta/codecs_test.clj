(ns greta.codecs-test
  (:require [greta.codecs :refer :all]
            [clojure.test :refer :all]
            [gloss.io :as io]))

(defn str->bytes [s]
  (-> s
      .getBytes
      bytes
      vec))

(defn round-trip?
  ([c m] (round-trip? c m m))
  ([c m m']
   (= m' (->> m
              (io/encode c)
              (io/decode c)))))

(deftest metadata-request-test
  (let [m {:api-version 0
           :correlation-id 1
           :client-id "greta-test"
           :topics ["my" "nice" "topics"]}

        r (conj {:api-key :metadata} m)]

    (is (round-trip? metadata-request m))
    (is (round-trip? request r m))))


(deftest metadata-response-test
  (let [b [{:node-id 1
             :host "example.com"
             :port 9092}]

        p [{:partition-error-code 0
            :partition-id 1
            :leader 1
            :replicas [1]
            :isr [1]}]

        t [{:topic-error-code 0
            :topic-name "greata-tests"
            :parition-metadata p}]

        m {:correlation-id 1
           :brokers b
           :topics t}]

    (is (round-trip? brokers b))
    (is (round-trip? partition-metadata p))
    (is (round-trip? topics t))
    (is (round-trip? metadata-response m))))

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
    (is (round-trip? request r p))

    ; Brittle? Yes. This value is spat out by the kafka log given the
    ; payload from above (computed crc = 2447778493)
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

(deftest fetch-request-test
  (let [f {:topic "greta-tests"
           :messages [{:partition 0
                       :fetch-offset 0
                       :max-bytes 640}]}

        fr {:api-version 0
            :correlation-id 1
            :client-id "greta-test"
            :replica-id 1
            :max-wait-time 1000
            :min-bytes 64
            :fetch [f]}

        r (conj {:api-key :fetch} fr)]

    (is (round-trip? fetch f))
    (is (round-trip? fetch-request fr))
    (is (round-trip? request r fr))))

(deftest fetch-response-test
  (let [ms {:offset 1
              :message {:magic-byte :zero
                        :attributes :none
                        :key (str->bytes "hello")
                        :value (str->bytes "there!")}}

        fm {:partition 1
            :error-code 0
            :highwater-mark-offset 0
            :message-set [ms]}

        fr {:correlation-id 1
            :fetch [{:topic-name "greta-tests"
                     :messages [fm]}]}]


    (is (round-trip? fetched-messages fm))
    (is (round-trip? fetch-response fr))))
