(ns greta.codecs.metadata
  (:require [gloss.core :refer :all]
            [greta.codecs.core :as c]))

(defcodec request
  (finite-frame :int32
   (ordered-map :api-key c/api-key
                :api-version :int16
                :correlation-id :int32
                :client-id c/sized-string
                :topics (repeated c/sized-string))))

(defcodec brokers
  (repeated
   (ordered-map :node-id :int32
                :host c/sized-string
                :port :int32)))


(defcodec partition-metadata
  (repeated
   (ordered-map :partition-error-code :int16
                :partition-id :int32
                :leader :int32
                :replicas (repeated :int32)
                :isr (repeated :int32))))


(defcodec topics
  (repeated
   (ordered-map :topic-error-code :int16
                :topic-name c/sized-string
                :parition-metadata partition-metadata)))

(defcodec response
  (finite-frame :int32
   (ordered-map :correlation-id :int32
                :brokers brokers
                :topics topics)))
