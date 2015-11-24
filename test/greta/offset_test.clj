(ns greta.offset-test
  (:require [greta.offset :refer :all]
            [clojure.test :refer :all]))


(deftest coordinator-test
  (is (every? @(coordinator "localhost" 9092 "my-group")
              [:error-code :host :port :coordinator-id])))
