(ns greta.offset
  (:require [greta.codecs.consumer-metadata :as md]
            [greta.core :as c]
            [manifold.deferred :as d]
            [manifold.stream :as s]))

(defn coordinator
  "Finds the host and port for the consumer offset manager."
  [host port consumer-group]

  (d/let-flow [c (c/client host
                           port
                           md/request
                           md/response)

               req (s/put! c {:api-key :consumer-metadata
                              :api-version 0
                              :correlation-id 1
                              :client-id "greta"
                              :consumer-group consumer-group})]

      (d/chain
       (s/take! c ::drained)

       (fn [r]
         (if (= r ::drained)
           "ERROR: Connection closed."

           (select-keys r [:error-code
                           :host
                           :port
                           :coordinator-id])))

       (fn [r]
         (.close c)
         (d/success-deferred r)))))
