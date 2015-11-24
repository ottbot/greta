(ns greta.codecs.core-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.core :refer :all]))


(defn round-trip?
  ([e m] (round-trip? e e m))
  ([e d m]
   (= m (->> m
             (io/encode e)
             (io/decode d)))))


(deftest identity-serde-test
  (let [s (identity-serde)]
    (is (= :ok (serialize s :ok)))
    (is (= :ok (deserialize s :ok)))))

(deftest string-serde-test
  (let [s (string-serde)]
    (is (= (->> {:a 1 :key "hey" :value "now"}
               (serialize s)
               (deserialize s))))))
