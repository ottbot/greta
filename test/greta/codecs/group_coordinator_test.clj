(ns greta.codecs.group-coordinator-test
  (:require [clojure.test :refer :all]
            [greta.codecs.core-test :refer [round-trip?]]
            [greta.codecs.group-coordinator :refer :all]))


(deftest request-test
  (let [r {:api-key :group-coordinator
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
