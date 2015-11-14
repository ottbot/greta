(ns greta.core
  (:require [aleph.tcp :as tcp]
            [gloss.io :as io]
            [greta.codecs.fetch :as fetch-codecs]
            [greta.codecs.metadata :as metadata-codecs]
            [greta.codecs.produce :as produce-codecs]
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


;; TODO dry
(defn metadata-client []
  (client "localhost" 9092
          metadata-codecs/request
          metadata-codecs/response))

(defn produce-client []
  (client "localhost" 9092
          produce-codecs/request
          produce-codecs/response))

(defn fetch-client []
  (client "localhost" 9092
          fetch-codecs/request
          fetch-codecs/response))


;; TODO make serde protocol.. provide default!
(defn str->bytes [s]
  (-> s
      .getBytes
      bytes
      vec))
