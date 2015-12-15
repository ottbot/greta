(ns greta.core-test
  (:require [clojure.test :refer :all]
            [greta.core :refer :all]
            [greta.serde :as serde]
            [manifold.stream :as s]))


;; Note: these tests are kind of temporary, while verifying that
;; 1) the raw api implemention works
;; 2) we can use a single connection for all of these things

(def ^:dynamic *conn* nil)

(defn with-client [f]
  (with-open [c @(client "localhost" 9092 (serde/string-serde))]
    (binding [*conn* c]
      (f))))


(use-fixtures :each with-client )


(deftest client-test
  (let [msg {:header {:api-key :metadata
                      :api-version 0
                      :client-id "greta"}

             :topics []}]

    (is @(s/put! *conn* msg))
    (is (every? @(s/take! *conn*)
                [:brokers :topics]))))


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


    (is @(s/put! *conn* msg))
    (is (= :none
           (get-in
            @(s/take! *conn*)
            [0 :results 0 :error-code])))))


(deftest fetch-test'
  (let [msg {:header {:api-key :fetch
                      :api-version 0
                      :correlation-id 1
                      :client-id "greta-test"}

             :replica-id -1
             :max-wait-time 1000
             :min-bytes 1
             :topics [{:topic "greta-tests"
                       :messages [{:partition 0
                                   :fetch-offset 0
                                   :max-bytes 1024}]}]}]


    (is @(s/put! *conn* msg))

    (let [res @(s/try-take! *conn* ::drained 1000 ::timeout)
          m (get-in res [0 :messages 0])]

      (is (= :none (:error-code m)))

      (is (or (= 0 (:highwater-mark-offset m))
              (not-empty (:message-set m)))))))


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


    (is @(s/put! *conn* msg))
    (is (= :none
           (get-in
            @(s/take! *conn*)
            [0 :partitions 0 :error-code])))))


(deftest offset-commit-test'
  (let [r {:header {:api-key :offset-commit
                    :api-version 2
                    :correlation-id 1
                    :client-id "greta-test"}
           :consumer-group-id "my-group"
           :consumer-group-generation-id 1
           :consumer-id "my-group"
           :retention-time -1 ;; would like to NOT set this?
           :topics [{:topic "greta-tests"
                     :partitions [{:partition-id 0
                                   :offset 101
                                   :metadata "funky"}]}]}]

    @(s/put! *conn* r)
    (is (some #{(get-in @(s/try-take! *conn* 1000)
                        [0 :partitions 0 :error-code])}

              [:illegal-generation
               :consumer-coordinator-not-available
               :not-coordinator-for-group]))))


(deftest offset-fetch-test'
  (let [r {:header {:api-key :offset-fetch
                    :api-version 1
                    :correlation-id 1
                    :client-id "greta-test"}
           :consumer-group "my-group"
           :topics [{:topic "greta-tests"
                     :partitions [0]}]}]

    @(s/put! *conn* r)
    (is (some #{(get-in @(s/try-take! *conn* 1000)
                        [0 :partitions 0 :error-code])}

              [:none :not-coordinator-for-group]))))


(deftest join-group-test'
  (let [r {:header {:api-key :join-group
                    :api-version 0
                    :correlation-id 1
                    :client-id "greta-test"}

           :group-id "my-group"
           :session-timeout 100
           :member-id ""
           :protocol-type "consumer"
           :group-protocols [{:protocol-name "dunno"
                              :protocol-metadata {:version 0
                                                  :subscription ["greta-tests"]
                                                  :user-data nil}}]}]

    @(s/put! *conn* r)
    (is (= :not-coordinator-for-group (:error-code @(s/try-take! *conn* 1000 ))))))
