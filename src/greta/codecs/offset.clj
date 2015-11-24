(ns greta.codecs.offset
  (:require [gloss.core :refer :all]
            [greta.codecs.core :as c]))


(defcodec topic-request
  (ordered-map
   :topic c/sized-string
   :partitions (repeated
                (ordered-map
                 :partition :int32
                 :time :int64
                 :max-number-of-offsets :int32))))

(defcodec request
  (finite-frame :int32
   (ordered-map
    :api-key c/api-key
    :api-version :int16
    :correlation-id :int32
    :client-id c/sized-string
    :replica-id :int32
    :topics (repeated topic-request))))


(defcodec topic-response
  (ordered-map
   :topic c/sized-string
   :partitions (repeated
                (ordered-map
                 :partition :int32
                 :error-code c/error
                 :offsets (repeated :int64)))))


(defcodec response
  (finite-frame
   :int32
   (ordered-map
    :correlation-id :int32
    :topics (repeated topic-response))))
