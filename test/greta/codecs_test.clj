(ns greta.codecs-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs :refer :all]
            [greta.serde :as serde]))

(defn round-trip?
  ([i m] (round-trip? i i m))
  ([i o m]
   (= m (->> m
             (io/encode i)
             (io/decode o)))))



(defmacro defapitest [title
                      api
                      request-example
                      response-example]
  `(deftest ~title
     (is (round-trip? (request ~api) ~request-example))
     (is (round-trip? (response ~api) ~response-example))))


(defapitest metadata-test
  (metadata)

  {:topics ["my" "nice" "topics"]}

  {:brokers [{:node-id 1
              :host "example.com"
              :port 9092}]
   :topics [{:topic-error-code :none
             :topic-name "greta-tests"
             :partition-metadata [{:partition-error-code :none
                                   :partition-id 1
                                   :leader 1
                                   :replicas [1]
                                   :isr [1]}]}]})


(defapitest offset-test
  (offset)

  {:replica-id 1
   :topics [{:topic "greta-tests"
             :partitions [{:partition 0
                           :time 100
                           :max-number-of-offsets 100}]}]}

  [{:topic "greta-tests"
    :partitions [{:partition 0
                  :error-code :none
                  :offsets [0]}]}])


(defapitest offset-fetch-test
  (offset-fetch)

  {:consumer-group "my-group"
   :topics [{:topic "greta-tests"
             :partitions [0]}]}

  [{:topic "greta-tests"
    :partitions [{:partition-id 0
                  :offset 101
                  :metadata "funky-town"
                  :error-code :none}]}])


(defapitest offset-commit-test
  (offset-commit)

  {:consumer-group-id "my-group"
   :consumer-group-generation-id 10
   :consumer-id "my-consumer"
   :retention-time 10000
   :topics [{:topic "greta-tests"
             :partitions [{:partition-id 0
                           :offset 101
                           :metadata "funky"}]}]}

  [{:topic "greta-tests"
    :partitions [{:partition-id 1
                  :error-code :none}]}])


(defapitest fetch-test
  (fetch (serde/string-serde))

  {:replica-id 1
   :max-wait-time 1000
   :min-bytes 64
   :topics [{:topic "greta-tests"
             :messages [{:partition 0
                         :fetch-offset 0
                         :max-bytes 640}]}]}


  [{:topic-name "greta-tests"
           :messages [{:partition 1
                       :error-code :none
                       :highwater-mark-offset 0
                       :message-set [{:offset 1
                                      :message {:magic-byte :zero
                                                :attributes :none
                                                :key "hello"
                                                :value "there!"}}]}]}])


(defapitest produce-test
  (produce (serde/string-serde))

  {:required-acks 1
   :timeout 1000
   :produce [{:topic "greta-tests"
              :messages [{:partition 0
                          :message-set [{:offset 1
                                         :message {:magic-byte :zero
                                                   :attributes :none
                                                   :key "hello"
                                                   :value "there!"}}]}]}]}

  [{:topic "greta-tests"
    :results [{:partition 0
               :offset 123
               :error-code :none}]}])


(defapitest group-coordinator-test
  (group-coordinator)

  {:consumer-group "my-group"}

  {:error-code :none
   :coordinator-id 10
   :host "example.com"
   :port 9092})


(defapitest join-group-test
  (join-group)

  {:group-id "my-group"
   :session-timeout 100
   :member-id "my-member"
   :protocol-type "consumer"
   :group-protocols [{:protocol-name "my-proto"
                      :protocol-metadata {:version 0
                                          :subscription ["dunno"]
                                          :user-data nil}}]}

  {:error-code :none
   :generation-id 10
   :group-protocol "my-proto"
   :leader-id "wut"
   :member-id "me"
   :members [{:id "me"
              :metadata nil}]})



(let [ms {:version 0
          :partition-assignment [{:topic "greta-tests"
                                  :partitions [0]}]
          :user-data nil}]

  (defapitest sync-group-test
    (sync-group)

    {:group-id "my-group"
     :generation-id 0
     :member-id "my-member"
     :group-assignment [{:member-id "my-member"
                         :member-assignment ms}]}

    {:error-code :none
     :member-assignment ms}))
