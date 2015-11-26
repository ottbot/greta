(ns greta.codecs.offset-commit
  (:require [gloss.core :refer :all]
            [greta.codecs.core :as c]))

;; This is for v1 of the OffsetFetch API

(defcodec request
  (finite-frame
   :int32
   (ordered-map
    :api-key c/api-key
    :api-version :int16
    :correlation-id :int32
    :client-id c/sized-string
    :consumer-group-id c/sized-string
    :consumer-group-generation-id :int32
    :consumer-id c/sized-string
    :topics (repeated
             (ordered-map
              :topic c/sized-string
              :partitions (repeated
                          (ordered-map
                           :partition-id :int32
                           :offset :int64
                           :timestamp :int64
                           :metadata c/sized-string)))))))


(defcodec response
  (finite-frame
   :int32
   (ordered-map
    :correlation-id :int32
    :topics (repeated
             (ordered-map
              :topic c/sized-string
              :partitions (repeated
                           (ordered-map
                            :partition-id :int32
                            :error-code c/error)))))))
