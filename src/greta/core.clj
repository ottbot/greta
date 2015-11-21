(ns greta.core
  (:require [aleph.tcp :as tcp]
            [gloss.io :as io]
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
  (d/chain
   (tcp/client {:host host
                :port port})

   (partial protocol-stream
            encoder
            decoder)))
