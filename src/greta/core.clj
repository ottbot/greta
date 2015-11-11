(ns greta.core
  (:require [aleph.tcp :as tcp]
            [gloss.io :as io]
            [greta.codecs :as codecs]
            [manifold.deferred :as d]
            [manifold.stream :as s]))


(defn protocol-stream
  [encoder decoder raw]
  (let [out (s/stream)]
    (s/connect
      (s/map #(io/encode encoder %) out)
      raw)
    (s/splice
      out
      (io/decode-stream raw decoder))))


(defn client
  [host port encoder decoder]
  (d/chain (tcp/client {:host host
                        :port port})
           (partial protocol-stream
                    encoder
                    decoder)))

(defn raw-client []
  (tcp/client {:host "localhost"
               :port 9092}))

(defn metadata-client []
  (client "localhost" 9092
          codecs/request codecs/metadata-response))

(defn produce-client []
  (client "localhost" 9092
          codecs/request codecs/produce-response))

(defn fetch-client []
  (client "localhost" 9092
          codecs/request
          codecs/fetch-response))

(defn str->bytes [s]
  (-> s
      .getBytes
      bytes
      vec))
