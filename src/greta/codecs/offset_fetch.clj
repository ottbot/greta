(ns greta.codecs.offset-fetch
  (:require [gloss.core :refer :all]
            [greta.codecs.core :as c]))

(defcodec request
  (finite-frame :int32
   (ordered-map
    :api-key c/api-key
    :api-version :int16
    :correlation-id :int32
    :client-id c/sized-string
    :consumer-group c/sized-string
    :topics (repeated (ordered-map
                       :topic c/sized-string
                       :partitions (repeated :int32))))))


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
                            :offset :int64
                            :metadata c/sized-string
                            :error-code c/error)))))))
