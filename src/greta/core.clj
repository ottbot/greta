(ns greta.core
  (:require [aleph.tcp :as tcp]
            [gloss.io :as io]
            [greta.codecs :as codecs]
            [manifold.deferred :as d]
            [manifold.stream :as s]))

(defn connection []
  (tcp/client {:host "localhost"
               :port 9092}))

(defn make-request [conn msg t]
  (s/try-put! conn
              (io/encode codecs/request msg)
              t
              ::timeout))

(defn request-metadata
  ([conn topics correlation-id]
   (request-metadata conn topics correlation-id 5000))
  ([conn topics correlation-id timeout]
   (let [msg {:api-key :metadata
              :api-version 0
              :correlation-id correlation-id
              :client-id "greta"
              :topics topics}]

     (make-request conn msg timeout))))

(defn take-response
  [conn codec t]
  (d/chain (s/try-take! conn t)
           (fn [m]
             (when m
               (io/decode codec m)))))

(defn metadata-response
  ([conn correlation-id] (metadata-response conn correlation-id 1000))
  ([conn correlation-id timeout]
   (take-response conn
                  codecs/metadata-response
                  timeout)))

(defn str->bytes [s]
  (-> s
      .getBytes
      bytes
      vec))


(defn produce-request [c correlation-id]
  (let [msg {:api-key :produce
             :api-version 0
             :correlation-id correlation-id
             :client-id "greta"
             :required-acks 1
             :timeout 1000
             :produce [{:topic "greta-tests"
                        :messages [{:partition 0
                                    :message-set [{:offset 0
                                                   :message
                                                   {:magic-byte :zero
                                                    :attributes :none
                                                    :key []
                                                    :value (str->bytes "there!")}}]}]}]}]
    (make-request c msg 100)))

(defn produce-response
  ([conn correlation-id]
   (produce-response conn correlation-id 1000))
  ([conn correlation-id timeout]
   (take-response conn
                  codecs/produce-response
                  timeout)))
