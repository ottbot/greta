(ns greta.metadata
  (:require [greta.codecs.metadata :as mdc]
            [greta.core :as c]
            [manifold.deferred :as d]
            [manifold.stream :as s]))



(defn client
  "A client to perform metadata requests

  .. an example request ..

  "
  ([{:keys [host port]}] (client host port) )
  ([host port]
              (c/client host
                        port
                        mdc/request
                        mdc/response)))


(defn topic-partition-leader
  "Takes a metadata client and returns a new one connected to the
  leader for a parition topic. Original connection will be closed."
  [host port topic partition-id]

  (d/let-flow [c (client host port)

               request (s/put! c {:api-key :metadata
                                  :api-version 0
                                  :correlation-id 1
                                  :client-id "greta"
                                  :topics [topic]})]

      (d/chain
       (s/take! c ::drained)

       (fn [r]
         (if (= r ::drained)
           "ERROR: Connection closed."

           (if-let [pm (get-in r [:topics 0 :partition-metadata])]

             (or (some->> pm
                          (filter #(= (:partition-id %) partition-id))
                          first
                          :leader
                          (nth (:brokers r)))

                 "ERROR: Parition not found")

             (str "ERROR: Kafka error: "
                  (name
                   (get-in r [:topics 0 :topic-error-code]))))))

       (fn [r]
         (.close c)
         (d/success-deferred r)))))
