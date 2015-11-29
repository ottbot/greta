(ns greta.codecs.produce
  (:require [gloss.core :refer :all]
            [gloss.io :as io]
            [greta.codecs.primitives :as p]
            [greta.serde :refer [serialize
                                  deserialize]]))

(defn crc [x]
  (.getValue
   (doto (java.util.zip.CRC32.)
     (.reset)
     (.update x))))


(defn message-body [serde]
  (compile-frame
   (ordered-map
    :magic-byte p/magic-byte
    :attributes p/compression
    :key p/bytes
    :value p/bytes)
   (partial serialize serde)
   (partial deserialize serde)))


(defn message-body-crc [serde x]
  (crc
   (io/contiguous
    (io/encode (message-body serde) x))))


(defn message [s]
  (compile-frame
   (header :uint32
           (fn [_]
             (message-body s))
           (partial message-body-crc s))))

(defn message-set [s]
  (compile-frame
   (repeated
    (ordered-map :offset :int64
                 :message (finite-frame :int32
                                        (message s)))
    :prefix :none)))

(defn produce [s]
  (compile-frame
   (ordered-map
    :topic p/string
    :messages (repeated
               (ordered-map
                :partition :int32
                :message-set (finite-frame
                              :int32
                              (message-set s)))))))

(defn request [serde]
  (ordered-map
   :required-acks :int16
   :timeout :int32
   :produce (repeated (produce serde))))


(defcodec partition-results
  (repeated
   (ordered-map
    :partition :int32
    :error-code p/error
    :offset :int64)))


(defcodec response
  (repeated
   (ordered-map
    :topic p/string
    :results partition-results)))
