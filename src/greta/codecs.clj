(ns greta.codecs
  (:require [byte-streams :as bs]
            [gloss.core :refer :all]
            [gloss.io :as io]))

(defcodec sized-string
  (finite-frame :int16-be
                (string :utf-8)))

(defcodec sized-bytes
  (repeated :byte :prefix :int32-be))

(defcodec api-key
  (enum :int16-be {:metadata 3
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
   (bs/to-byte-array
    (io/encode message-body x))))

(defcodec message
  (header :uint32-be
          (fn [_]
            message-body)
          message-body-crc))


(defcodec message-set
  (repeated
   (ordered-map :offset :int64-be
                :message (finite-frame :int32-be
                                       message))
   :prefix :none))

(defcodec produce
  (ordered-map
   :topic sized-string
   :messages (repeated
              (ordered-map
               :partition :int32-be
               :message-set (finite-frame :int32-be
                                          message-set)))))

(defcodec produce-request
  (ordered-map
   :api-version :int16-be
   :correlation-id :int32-be
   :client-id sized-string
   :required-acks :int16-be
   :timeout :int32-be
   :produce (repeated produce)))


(defcodec metadata-request
  (ordered-map :api-version :int16-be
               :correlation-id :int32-be
               :client-id sized-string
               :topics (repeated sized-string)))

(defcodec fetch
  (ordered-map
   :topic sized-string
   :messages (repeated
              (ordered-map
               :partition :int32-be
               :fetch-offset :int64-be
               :max-bytes :int32-be))))

(defcodec fetch-request
  (ordered-map :api-version :int16-be
               :correlation-id :int32-be
               :client-id sized-string
               :replica-id :int32-be
               :max-wait-time :int32-be
               :min-bytes :int32-be
               :fetch (repeated fetch)))

(defcodec request
  (finite-frame :int32-be
                (header api-key
                        {:metadata metadata-request
                         :produce produce-request
                         :fetch fetch-request}
                        :api-key)))


(defcodec fetched-messages
  (ordered-map :partition :int32-be
               :error-code :int16-be
               :highwater-mark-offset :int64-be
               :message-set (finite-frame :int32-be
                                          message-set)))

(defcodec fetch-response
  (ordered-map
   :correlation-id :int32-be
   :fetch (repeated
           (ordered-map
            :topic-name sized-string
            :messages (repeated fetched-messages)))))

(defcodec partition-results
  (repeated
   (ordered-map
    :parition :int32-be
    :error-code :int16-be
    :offset :int64-be)))


(defcodec produce-response
  (finite-frame :int32-be
                (ordered-map
                 :correlation-id :int32-be
                 :produce (repeated
                           (ordered-map
                            :topic sized-string
                            :results partition-results)))))


(defcodec brokers
  (repeated
   (ordered-map :node-id :int32-be
                :host sized-string
                :port :int32-be)))


(defcodec partition-metadata
  (repeated
   (ordered-map :partition-error-code :int16-be
                :partition-id :int32-be
                :leader :int32-be
                :replicas (repeated :int32-be)
                :isr (repeated :int32-be))))


(defcodec topics
  (repeated (ordered-map :topic-error-code :int16-be
                         :topic-name sized-string
                         :parition-metadata partition-metadata)))

(defcodec metadata-response
  (finite-frame :int32-be
                (ordered-map :correlation-id :int32-be
                             :brokers brokers
                             :topics topics)))
