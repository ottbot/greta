(ns greta.core-test
  (:require [clojure.test :refer :all]
            [greta.core :refer :all]))

(deftest metadata-test
  (with-open [c @(connection)]
    (is @(request-metadata c [] 1))
    (is (= 1 (:correlation-id
              @(metadata-response c 1))))))


;; (deftest produce-test
;;   (with-open [c @(connection)]
;;     (is @(produce-request c 1))
;;     (is (= 1 @(produce-response c 1)))))
