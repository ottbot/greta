(ns greta.core
  (:require [aleph.tcp :as tcp]
            [gloss.io :as io]
            [greta.codecs :as codecs]
            [greta.serde :as serde]
            [manifold.deferred :as d]
            [manifold.stream :as s]))


(def client-name (or (System/getenv "GRETA_CLIENT_NAME")
                     "greta-tests"))


(defn client
  "Get an Aleph TCP client to perform Kafka API requests.

  A SerDe for Kafka message key and value can optionally be
  provided. A string serde is the default."

  ([host port] (client host port (serde/string-serde)))

  ([host port serde]
   (d/chain

    (tcp/client {:host host
                 :port port})

    (fn [raw]
      (let [out (s/stream)
            codec (codecs/correlated serde)]

        (s/connect
         (s/map
          (partial io/encode
                   (codecs/request codec)) out) raw)

        (s/splice
         out
         (io/decode-stream raw
                           (codecs/response codec))))))))


(defn metadata [connection topic]
  (when @(s/put! connection {:header {:api-key :metadata
                                      :api-version 0
                                      :client-id client-name}
                             :topics [topic]})
    (s/take! connection)))
