(ns greta.codecs.consumer-metadata
  (:require [greta.codecs.core :as c]
            [gloss.core :refer :all]))

(defcodec request
  (finite-frame
   :int32
   (ordered-map
    :api-key c/api-key
    :api-version :int16
    :correlation-id :int32
    :client-id c/sized-string
    :consumer-group c/sized-string)))

(defcodec response
  (finite-frame
   :int32
   (ordered-map
    :correlation-id :int32
    :error-code c/error
    :coordinator-id :int32
    :host c/sized-string
    :port :int32)))
