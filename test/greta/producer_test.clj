(ns greta.producer-test
  (:require [clojure.test :refer :all]
            [greta.core :as c]
            [greta.producer :refer :all]
            [greta.serde :as serde]
            [manifold.stream :as s]))


(deftest leader-pool-test
  "This is test is temporary"
  (is (= 1 (count
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
