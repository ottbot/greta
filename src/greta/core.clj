(ns greta.core
  (:require [aleph.tcp :as tcp]
            [gloss.io :as io]
            [greta.codecs :as codecs]
            [greta.serde :as serde]
            [manifold.deferred :as d]
            [manifold.stream :as s]))


(defn client
  "A client to perform 'raw' kafka api requests. Do explain.

  .. an example request ..

  "
  ([{:keys [host port serde] :or {host "localhost"
                                  port 9092
                                  serde (serde/string-serde)}}]
   (client host port serde))

  ([host port] (client host port (serde/string-serde)))

  ([host port serde]
   (d/chain

    (tcp/client {:host host
                 :port port})

    (fn [raw]
      (let [out (s/stream)
            codec (codecs/correlated-codecs serde)]

        (s/connect
         (s/map
          (partial io/encode
                   (codecs/request codec)) out) raw)

        (s/splice
         out
         (io/decode-stream raw
                           (codecs/response codec))))))))
