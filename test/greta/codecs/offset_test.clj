(ns greta.codecs.offset-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.offset :refer :all]
            [greta.codecs.core-test :refer [round-trip?]]))

(deftest request-test
  (let [t {:topic "greta-tests"
           :partitions [{:partition 0
                         :time 100
                         :max-number-of-offsets 100}]}

        r {:api-key :offset
           :api-version 1
           :correlation-id 1
           :client-id "greta-tests"
           :replica-id 1
           :topics [t]}]

    (is (round-trip? topic-request t))
    (is (round-trip? request r))))


(deftest response-test
  (let [t {:topic "greta-tests"
           :partitions [{:partition 0
                         :error-code 0
                         :offsets [0]}]}

        r {:correlation-id 1
           :topics [t]}]

    (is (round-trip? topic-response t))
    (is (round-trip? response r))))
