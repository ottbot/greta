(ns greta.codecs.core
  (:require [gloss.core :refer :all]))

(defcodec sized-string
  (finite-frame :int16
                (string :utf-8)))

(defcodec sized-bytes
  (repeated :byte :prefix :int32))

(defcodec api-key
  (enum :int16 {:metadata 3
                :produce 0
                :fetch 1
                :offset 2
                :consumer-metadata 10}))

(defcodec magic-byte
  (enum :byte {:zero 0}))

(defcodec compression
  (enum :byte {:none 0
               :gzip 1
               :snappy 2}))

(defcodec error
  (enum :int16 {:none 0
                :unknown -1
                :offset-out-of-range 1
                :invalid-message 2
                :unknown-topic-or-partition 3
                :invalid-message-size 4
                :leader-not-available 5
                :not-leader-for-partition 6
                :request-timed-out 7
                :broker-not-available 8
                :replica-not-available 9
                :message-size-too-large 10
                :stale-controller-epoch 11
                :offset-metadata-too-large 12
                :offset-load-in-progress 14
                :consumer-coordinator-not-available 15
                :not-coordinator-for-consumer-code 16}))


(defn crc [x]
  (.getValue
   (doto (java.util.zip.CRC32.)
     (.reset)
     (.update x))))


(defprotocol KafkaSerde
  "Takes a map containing a :key and :value, and returns the same map
  but with (de)serialized values for those keys"
  (serialize [this v])
  (deserialize [this v]))

(defn identity-serde
  "Doesn't modify values."
  []
  (reify
    KafkaSerde
    (serialize [_ m] m)
    (deserialize [_ m] m)))


(defn string-serde
  "Takes and returns strings"
  []
  (reify
    KafkaSerde
    (serialize [_ m]
      (letfn [(xf [v] (-> v
                          .getBytes
                          bytes
                          vec))]
        (-> m
            (update :key xf)
            (update :value xf))))
    (deserialize [_ m]
      (letfn [(xf [v] (if (= (first v) -1)
                        nil
                        (transduce (map char) str v)))]
        (-> m
            (update :key xf)
            (update :value xf))))))
