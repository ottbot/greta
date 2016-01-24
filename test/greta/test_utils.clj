(ns greta.test-utils
  (:require [greta.core :as core]
            [greta.serde :as serde]))


(def kafka-host
  (or (System/getenv "KAFKA_HOST")
      "localhost"))

(def kafka-port
  (or (System/getenv "KAFKA_PORT")
      9092))
