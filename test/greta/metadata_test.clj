(ns greta.metadata-test
  (:require [clojure.test :refer :all]
            [greta.metadata :refer :all]
            [manifold.stream :as s]))

(deftest client-test
  (let [cid 1
        msg {:api-key :metadata
             :api-version 0
             :correlation-id cid
             :client-id "greta"
             :topics []}]

    (with-open [c @(client "localhost" 9092)]
      (is @(s/put! c msg))
      (is (= cid (:correlation-id
                  @(s/take! c)))))))
