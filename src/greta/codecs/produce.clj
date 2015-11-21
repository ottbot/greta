(ns greta.codecs.produce
  (:require [gloss.core :refer :all]
            [gloss.io :as io]
            [greta.codecs.core :as c]))


(defn message-body [serde]
  (compile-frame
   (ordered-map
    :magic-byte c/magic-byte
    :attributes c/compression
    :key c/sized-bytes
    :value c/sized-bytes)
   (partial c/serialize serde)
   (partial c/deserialize serde)))

(defn message-body-crc [serde x]
  (c/crc
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
    :topic c/sized-string
    :messages (repeated
               (ordered-map
                :partition :int32
                :message-set (finite-frame
                              :int32
                              (message-set s)))))))

(defn request [serde]
  (compile-frame
   (finite-frame
    :int32
    (ordered-map
     :api-key c/api-key
     :api-version :int16
     :correlation-id :int32
     :client-id c/sized-string
     :required-acks :int16
     :timeout :int32
     :produce (repeated (produce serde))))))


(defcodec partition-results
  (repeated
   (ordered-map
    :parition :int32
    :error-code :int16
    :offset :int64)))


(defcodec response
  (finite-frame :int32
                (ordered-map
                 :correlation-id :int32
                 :produce (repeated
                           (ordered-map
                            :topic c/sized-string
                            :results partition-results)))))
