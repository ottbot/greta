(ns greta.codecs-test
  (:require [greta.codecs :refer :all]
            [clojure.test :refer :all]
            [gloss.io :as io]))



(defn round-trip? [c m]
  (= m (->> m
            (io/encode c)
            (io/decode c))))

(deftest message-test
  (let [m {:crc 0
           :magic-byte :zero
           :attributes :none
           :key "hello"
           :value "hello"}]
    (is (round-trip? message m))))
