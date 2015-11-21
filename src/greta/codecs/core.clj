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
                :offset 2}))

(defcodec magic-byte
  (enum :byte {:zero 0}))

(defcodec compression
  (enum :byte {:none 0
               :gzip 1
               :snappy 2}))

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
