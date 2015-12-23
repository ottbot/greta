(ns greta.core-test
  (:require [clojure.test :refer :all]
            [greta.core :refer :all]
            [greta.serde :as serde]
            [manifold.stream :as s]))



(def ^:dynamic *conn* nil)

(use-fixtures :once
  (fn [f]
    (with-open [c @(client "localhost" 9092 (serde/string-serde))]
      (binding [*conn* c]
        (f)))) )


(deftest metadata-test
  (is (every?
       @(metadata *conn* "greta-tests")
       [:brokers :topics])))

(deftest client-test
  ;; As higher-level client fn are added, these tests can be removed.

  (testing "fetch"
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

        (is (contains?
             #{:none :offset-out-of-range}
             (:error-code m)))

        (is (or (> 1 (:highwater-mark-offset m))
                (not-empty (:message-set m)))))))


  (testing "offset"
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


  (testing "offset-commit"
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
      (is (contains?

           #{:illegal-generation
             :consumer-coordinator-not-available
             :not-coordinator-for-group}

           (get-in @(s/try-take! *conn* 1000)
                   [0 :partitions 0 :error-code])))))


  (testing "offset-fetch"
    (let [r {:header {:api-key :offset-fetch
                      :api-version 1
                      :correlation-id 1
                      :client-id "greta-test"}
             :consumer-group "my-group"
             :topics [{:topic "greta-tests"
                       :partitions [0]}]}]

      @(s/put! *conn* r)
      (is (contains?

           #{:none :not-coordinator-for-group}

           (get-in @(s/try-take! *conn* 1000)
                   [0 :partitions 0 :error-code])))))

  (testing "join-group"
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
      (is (= :not-coordinator-for-group
             (:error-code @(s/try-take! *conn* 1000 ))))))


  (testing "sync-group"
    (let [r {:header {:api-key :sync-group
                      :api-version 0
                      :correlation-id 0
                      :client-id "greta-test"}

             :group-id "my-group"
             :generation-id 0
             :member-id "my-member"
             :group-assignment [{:member-id "my-member"
                                 :member-assignment {:version 0
                                                     :partition-assignment [{:topic "greta-tests"
                                                                             :partitions [0]}]
                                                     :user-data nil}}]}]

      @(s/put! *conn* r)
      (is (= :not-coordinator-for-group
             (:error-code @(s/try-take! *conn* 1000 ))))))


  (testing "heartbeat"
    (let [r {:header {:api-key :heartbeat
                      :api-version 0
                      :correlation-id 0
                      :client-id "greta-test"}

             :group-id "my-group"
             :generation-id 0
             :member-id "my-member"}]

      @(s/put! *conn* r)

      (is (= :not-coordinator-for-group
             @(s/try-take! *conn* 1000 )))))

  (testing "leave-group"
    (let [r {:header {:api-key :leave-group
                      :api-version 0
                      :correlation-id 0
                      :client-id "greta-test"}

             :group-id "my-group"
             :member-id "my-member"}]

      @(s/put! *conn* r)

      (is (= :not-coordinator-for-group
             @(s/try-take! *conn* 1000 )))))


  (testing "list-groups"
    (let [r {:header {:api-key :list-groups
                      :api-version 0
                      :correlation-id 0
                      :client-id "greta-test"}}]

      @(s/put! *conn* r)

      (is (= :none (:error-code
                    @(s/try-take! *conn* 1000 ))))))


  (testing "describe-groups"
    (let [r {:header {:api-key :describe-groups
                      :api-version 0
                      :correlation-id 0
                      :client-id "greta-test"}

             :group-ids ["my-group"]}]

      @(s/put! *conn* r)

      (is (= :not-coordinator-for-group
             (get-in
              @(s/try-take! *conn* 1000 )
              [0 :error-code]))))))
