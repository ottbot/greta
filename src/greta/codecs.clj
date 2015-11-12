(ns greta.codecs
  (:require [gloss.core :refer :all]
            [gloss.io :as io]
            [greta.codecs.core :refer :all]))



(defcodec message-body
  (ordered-map
   :magic-byte magic-byte
   :attributes compression
   :key sized-bytes
   :value sized-bytes))

(defn message-body-crc [x]
  (crc
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
   :topic sized-string
   :messages (repeated
              (ordered-map
               :partition :int32
               :message-set (finite-frame :int32
                                          message-set)))))

(defcodec produce-request
  (ordered-map
   :api-version :int16
   :correlation-id :int32
   :client-id sized-string
   :required-acks :int16
   :timeout :int32
   :produce (repeated produce)))





(defcodec request
  (finite-frame :int32
                (header api-key
                        {:metadata metadata-request
                         :produce produce-request
                         }
                        :api-key)))





(defcodec partition-results
  (repeated
   (ordered-map
    :parition :int32
    :error-code :int16
    :offset :int64)))


(defcodec produce-response
  (finite-frame :int32
                (ordered-map
                 :correlation-id :int32
                 :produce (repeated
                           (ordered-map
                            :topic sized-string
                            :results partition-results)))))
