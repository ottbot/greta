(ns greta.serde-test
  (:require [greta.serde :refer :all]
            [clojure.test :refer :all]))

(deftest identity-serde-test
  (let [s (identity-serde)]
    (is (= :ok (serialize s :ok)))
    (is (= :ok (deserialize s :ok)))))

(deftest string-serde-test
  (let [s (string-serde)]
    (is (= (->> {:a 1 :key "hey" :value "now"}
               (serialize s)
               (deserialize s))))))
