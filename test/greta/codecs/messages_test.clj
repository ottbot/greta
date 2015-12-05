(ns greta.codecs.messages-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.messages :refer :all]
            [greta.codecs-test :refer [round-trip?]]
            [greta.serde :as serde]))

(def my-message
  {:crc 123
   :magic-byte :zero
   :attributes :none
   :key nil
   :value "HEY!"})


(deftest message-test
  (is (round-trip?
       (message (serde/string-serde)) my-message)))

(deftest message-set-test
  (is (round-trip?
       (message-set (serde/string-serde))
       [{:offset 1 :message my-message}])))
