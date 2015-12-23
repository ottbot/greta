(ns greta.producer-test
  (:require [clojure.test :refer :all]
            [greta.core :as c]
            [greta.producer :refer :all]
            [greta.serde :as serde]
            [manifold.deferred :as d]
            [manifold.stream :as s]))


(defn dummy [v]
  (let [in (s/stream)]
    (s/consume identity in)

    (d/success-deferred
     (s/splice
      in
      (s/->source (repeat v))))))

(deftest leader-pool-test
  (let [pool-with #(-> {:brokers [{:node-id 0
                                   :host "localhost"
                                   :port 9092}

                                  {:node-id 1
                                   :host "localhost"
                                   :port 9092}]

                        :topics [{:topic-error-code :none
                                  :topic-name "greta-tests"
                                  :partition-metadata []}]}

                       (assoc-in [:topics 0 :partition-metadata] %)
                       dummy
                       (leader-pool "greta-tests" (serde/string-serde)))]


    (testing "where each partition has the same leader"
      (are [x f] (= x
                    (f @(pool-with [{:partition-error-code :none
                                     :partition-id 0
                                     :leader 0}

                                    {:partition-error-code :none
                                     :partition-id 1
                                     :leader 0}

                                    {:partition-error-code :none
                                     :partition-id 2
                                     :leader 0}])))

        1 (comp count distinct)
        3 count))

    (testing "where partitions have different leaders"
      (are [x f] (= x
                    (f @(pool-with [{:partition-error-code :none
                                     :partition-id 0
                                     :leader 0}

                                    {:partition-error-code :none
                                     :partition-id 1
                                     :leader 1}

                                    {:partition-error-code :none
                                     :partition-id 2
                                     :leader 0}])))


        2 (comp count distinct)
        3 count))


    (testing "a partition error"
      (is (thrown-with-msg? Exception
                            #"Partition error"
                            @(pool-with [{:partition-error-code :bad-thing
                                          :partition-id 0
                                          :leader 0}]))))

    (testing "a rebalance timeout"
      (is (thrown-with-msg? Exception
                            #"Rebalance timeout"
                            @(pool-with [{:partition-error-code :none
                                          :partition-id 0
                                          :leader -1}])))))

  (testing "a topic error"
    (is (thrown-with-msg?
         Exception
         #"Topic error"
         @(leader-pool
           (dummy {:topics [{:topic-error-code :bad-stuff}]})
           "greta-tests" (serde/string-serde)))))


  (testing "a real connection"
    (is (not-empty
         @(leader-pool "localhost"
                       9092
                       "greta-tests"
                       (serde/string-serde))))))

(deftest stream-test
  (with-open [p (stream "localhost" 9092 "greta-tests")]

    (dotimes [_ 5]
      (s/put! p "hello" ))

    (is (< (map :offset
                (s/stream->seq p 1000))))))
