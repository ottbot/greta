(ns greta.offset-test
  (:require [clojure.test :refer :all]
            [greta.offset :refer :all]
            [manifold.stream :as s]))


;; Pending CI fixes..

;; (deftest coordinator-test
;;   (is (every? @(coordinator "localhost" 9092 "my-group")
;;               [:error-code :host :port :coordinator-id])))
