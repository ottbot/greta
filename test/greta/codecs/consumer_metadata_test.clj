(ns greta.codecs.consumer-metadata-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.core :as c]
            [greta.codecs.core-test :refer [round-trip?]]
            [greta.codecs.consumer-metadata :refer :all]))


(deftest request-test
  (let [r {:api-key :consumer-metadata
           :api-version 0
           :correlation-id 1
           :client-id "greta-test"
           :consumer-group "my-group"}]

    (is (round-trip? request r))))

(deftest response-test
  (let [r {:correlation-id 1
           :error-code :none
           :coordinator-id 42
           :host "example.com"
           :port 9092}]

    (is (round-trip? response r))))
