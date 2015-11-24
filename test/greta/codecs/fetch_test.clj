(ns greta.codecs.fetch-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.core :as c]
            [greta.codecs.core-test :refer [round-trip?]]
            [greta.codecs.fetch :refer :all]))

(deftest fetch-request-test
  (let [f {:topic "greta-tests"
           :messages [{:partition 0
                       :fetch-offset 0
                       :max-bytes 640}]}

        r {:api-key :fetch
           :api-version 0
           :correlation-id 1
           :client-id "greta-test"
           :replica-id 1
           :max-wait-time 1000
           :min-bytes 64
           :topics [f]}]

    (is (round-trip? fetch-topic f))
    (is (round-trip? request r))))

(deftest fetch-response-test
  (let [ms {:offset 1
            :size 1000
            :crc 102121
            :magic-byte :zero
            :attributes :none
            :key "hello"
            :value "there!"}

        fm {:partition 1
            :error-code :none
            :highwater-mark-offset 0
            :message-set [ms]}

        fr {:correlation-id 1
            :topics [{:topic-name "greta-tests"
                      :messages [fm]}]}]


    (is (round-trip? (fixed-size-messages (c/string-serde))
                     (optimized-messages (c/string-serde))
                     fm))

    (is (round-trip? (fixed-size-response (c/string-serde))
                     (response (c/string-serde)) fr))))
