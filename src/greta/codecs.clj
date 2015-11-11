(ns greta.codecs
  (:require [gloss.core :refer :all]
            [gloss.io :as io]))

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


(defcodec metadata-request
  (ordered-map :api-version :int16
               :correlation-id :int32
               :client-id sized-string
               :topics (repeated sized-string)))

(defcodec fetch
  (ordered-map
   :topic sized-string
   :messages (repeated
              (ordered-map
               :partition :int32
               :fetch-offset :int64
               :max-bytes :int32))))

(defcodec fetch-request
  (ordered-map :api-version :int16
               :correlation-id :int32
               :client-id sized-string
               :replica-id :int32
               :max-wait-time :int32
               :min-bytes :int32
               :fetch (repeated fetch)))

(defcodec request
  (finite-frame :int32
                (header api-key
                        {:metadata metadata-request
                         :produce produce-request
                         :fetch fetch-request}
                        :api-key)))


(defcodec fetched-messages
  (ordered-map :partition :int32
               :error-code :int16
               :highwater-mark-offset :int64
               :message-set (finite-frame :int32
                                          message-set)))

(defcodec fetch-response
  (finite-frame :int32
                (ordered-map
                 :correlation-id :int32
                 :fetch (repeated
                         (ordered-map
                          :topic-name sized-string
                          :messages (repeated fetched-messages))))))

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


(defcodec brokers
  (repeated
   (ordered-map :node-id :int32
                :host sized-string
                :port :int32)))


(defcodec partition-metadata
  (repeated
   (ordered-map :partition-error-code :int16
                :partition-id :int32
                :leader :int32
                :replicas (repeated :int32)
                :isr (repeated :int32))))


(defcodec topics
  (repeated (ordered-map :topic-error-code :int16
                         :topic-name sized-string
                         :parition-metadata partition-metadata)))

(defcodec metadata-response
  (finite-frame :int32
                (ordered-map :correlation-id :int32
                             :brokers brokers
                             :topics topics)))
