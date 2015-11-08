(ns greta.codecs
  (:require [gloss.core :refer :all]))

(defcodec sized-string (finite-frame :int16-be
                                     (string :utf-8)))


(defcodec api-key (enum :int16-be {:metadata 3 :produce 0}))

(defcodec topics (repeated sized-string))

(defcodec metadata-request
  (ordered-map :api-version :int16-be
               :correlation-id :int32-be
               :client-id sized-string
               :topics topics))

(defcodec magic-byte (enum :byte {:zero 0}))

(defcodec compression (enum :byte {:none 0
                                   :gzip 1
                                   :snappy 2}))
(defcodec message
  (ordered-map :crc :int32
               :magic-byte magic-byte ; replace with enum
               :attributes compression
               :key sized-string
               :value sized-string))


(defcodec produce-response
  (finite-frame :int32-be
                (ordered-map :correlation-id :int32-be
                             :topic sized-string
                             :parition :int32-be
                             :error-code :int16-be
                             :offset :int64-be)))

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
                (ordered-map :correlation-id :int32-be ;; end request header
                             :brokers brokers
                             :topics topics)))
