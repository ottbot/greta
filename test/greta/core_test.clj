(ns greta.core-test
  (:require [clojure.test :refer :all]
            [greta.core :refer :all]))

(deftest metadata-test
  (with-open [c @(connection)]
    (is @(request-metadata c [] 1))
    (is (= 1 (:correlation-id
              @(metadata-response c 1))))))


(deftest produce-test
  (with-open [c @(connection)]
    (is @(produce-request c 2))
    (is (= 2 (:correlation-id
              @(produce-response c 2))))))
