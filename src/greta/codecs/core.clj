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
                :fetch 1}))

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
