(ns greta.codecs.produce
  (:require [gloss.core.protocols :as p]
            [gloss.core :refer :all]
            [gloss.io :as io]
            [greta.codecs.core :as c]))

(defcodec message-body
  (ordered-map
   :magic-byte c/magic-byte
   :attributes c/compression
   :key c/sized-bytes
   :value c/sized-bytes))

(defn message-body-crc [x]
  (c/crc
   (io/contiguous
    (io/encode message-body x))))


(defcodec message
  (header :uint32
          (fn [_]
            message-body)
          message-body-crc))

(defcodec message-set
  (repeated
   (ordered-map :offset :int64
                :message (finite-frame :int32
                                       message))
   :prefix :none))

(defcodec produce
  (ordered-map
   :topic c/sized-string
   :messages (repeated
              (ordered-map
               :partition :int32
               :message-set (finite-frame
                             :int32
                             message-set)))))

(defcodec request
  (finite-frame :int32
   (ordered-map
    :api-key c/api-key
    :api-version :int16
    :correlation-id :int32
    :client-id c/sized-string
    :required-acks :int16
    :timeout :int32
    :produce (repeated produce))))


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
