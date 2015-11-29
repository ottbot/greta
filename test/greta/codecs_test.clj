(ns greta.codecs-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs :refer :all]))

(defn round-trip?
  ([i m] (round-trip? i i m))
  ([i o m]
   (= m (->> m
             (io/encode i)
             (io/decode o)))))

(deftest metadata-test
  (let [api (metadata)

        req {:topics ["my" "nice" "topics"]}

        res {:brokers [{:node-id 1
                        :host "example.com"
                        :port 9092}]

             :topics [{:topic-error-code :none
                       :topic-name "greata-tests"
                       :partition-metadata [{:partition-error-code :none
                                             :partition-id 1
                                             :leader 1
                                             :replicas [1]
                                             :isr [1]}]}]}]

    (is (round-trip? (request api) req))
    (is (round-trip? (response api) res))))


;;; OFFSET

;; (deftest request-test
;;   (let [t {:topic "greta-tests"
;;            :partitions [{:partition 0
;;                          :time 100
;;                          :max-number-of-offsets 100}]}

;;         r {:api-key :offset
;;            :api-version 1
;;            :correlation-id 1
;;            :client-id "greta-tests"
;;            :replica-id 1
;;            :topics [t]}]

;;     (is (round-trip? topic-request t))
;;     (is (round-trip? request r))))


;; (deftest response-test
;;   (let [t {:topic "greta-tests"
;;            :partitions [{:partition 0
;;                          :error-code :none
;;                          :offsets [0]}]}

;;         r {:correlation-id 1
;;            :topics [t]}]

;;     (is (round-trip? topic-response t))
;;     (is (round-trip? response r))))


;; OFFSET FETCH
;; (deftest request-test
;;   (let [r {:api-key :offset-fetch
;;            :api-version 0
;;            :correlation-id 1
;;            :client-id "greta-test"
;;            :consumer-group "my-group"
;;            :topics [{:topic "greta-tests"
;;                      :partitions [0]}]}]

;;     (is (round-trip? request r))))

;; (deftest response-test
;;   (let [r {:correlation-id 1
;;            :topics [{:topic "greta-tests"
;;                      :partitions [{:partition-id 0
;;                                    :offset 101
;;                                    :metadata "funky-town"
;;                                    :error-code :none}]}]}]

;;     (is (round-trip? response r))))


;; OFFSET COMMIT
;; (deftest request-test
;;   (let [r {:api-key :offset-commit
;;            :api-version 0
;;            :correlation-id 1
;;            :client-id "greta-test"
;;            :consumer-group-id "my-group"
;;            :consumer-group-generation-id 10
;;            :consumer-id "my-consumer"
;;            :retention-time 10000
;;            :topics [{:topic "greta-tests"
;;                      :partitions [{:partition-id 0
;;                                   :offset 101
;;                                   :metadata "funky"}]}]}]

;;     (is (round-trip? request r))))


;; (deftest response-test
;;   (let [r {:correlation-id 1
;;            :topics [{:topic "greta-tests"
;;                      :partitions [{:partition-id 1
;;                                    :error-code :none}]}]}]

;;     (is (round-trip? response r))))


;;;;; FETCH

;; (deftest request-test
;;   (let [f {:topic "greta-tests"
;;            :messages [{:partition 0
;;                        :fetch-offset 0
;;                        :max-bytes 640}]}

;;         r {:api-key :fetch
;;            :api-version 0
;;            :correlation-id 1
;;            :client-id "greta-test"
;;            :replica-id 1
;;            :max-wait-time 1000
;;            :min-bytes 64
;;            :topics [f]}]

;;     (is (round-trip? fetch-topic f))
;;     (is (round-trip? request r))))

;; (deftest response-test
;;   (let [ms {:offset 1
;;             :size 1000
;;             :crc 102121
;;             :magic-byte :zero
;;             :attributes :none
;;             :key "hello"
;;             :value "there!"}

;;         fm {:partition 1
;;             :error-code :none
;;             :highwater-mark-offset 0
;;             :message-set [ms]}

;;         fr {:correlation-id 1
;;             :topics [{:topic-name "greta-tests"
;;                       :messages [fm]}]}

;;         serde (greta.serde/string-serde)]


;;     (is (round-trip? (fixed-size-messages serde)
;;                      (optimized-messages serde)
;;                      fm))

;;     (is (round-trip? (fixed-size-response serde)
;;                      (response serde) fr))))


;; PRODUCE!!
;; (deftest produce-request-test
;;   (let [m {:magic-byte :zero
;;            :attributes :none
;;            :key "hello"
;;            :value "there!"}

;;         ms [{:offset 1
;;              :message m}]

;;         r {:api-key :produce
;;            :api-version 0
;;            :correlation-id 1
;;            :client-id "greta-test"
;;            :required-acks 1
;;            :timeout 1000
;;            :produce [{:topic "greta-tests"
;;                        :messages [{:partition 0
;;                                    :message-set ms}]}]}]

;;     (is (round-trip? (message (string-serde)) m))
;;     (is (round-trip? (message-set (string-serde)) ms))
;;     (is (round-trip? (request (string-serde)) r))


;;     ;; Brittle? Yes. This value is spat out by the kafka log given the
;;     ;; payload from above (computed crc = 2447778493)
;;     (testing "CRC calculation given Kafka computed value."
;;       (let [expected  2447778493]
;;         (is (= expected (message-body-crc (string-serde) m)))))))

;; (deftest produce-response-test
;;   (let [m {:correlation-id 1
;;            :produce [{:topic "greta-tests"
;;                       :results [{:partition 0
;;                                  :offset 123
;;                                  :error-code :none}]}]}]

;;     (is (round-trip? response m))))



;;; GROUP COORDINATOR
;; (deftest request-test
;;   (let [r {:api-key :group-coordinator
;;            :api-version 0
;;            :correlation-id 1
;;            :client-id "greta-test"
;;            :consumer-group "my-group"}]

;;     (is (round-trip? request r))))

;; (deftest response-test
;;   (let [r {:correlation-id 1
;;            :error-code :none
;;            :coordinator-id 42
;;            :host "example.com"
;;            :port 9092}]

;;     (is (round-trip? response r))))
