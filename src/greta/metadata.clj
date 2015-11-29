(ns greta.metadata
  (:require [greta.core :as c]
            [manifold.deferred :as d]
            [manifold.stream :as s]))

(defn topic-partition-leader
  "Takes a metadata client and returns a new one connected to the
  leader for a partition topic. Original connection will be closed."
  [host port topic partition-id]

  (d/let-flow [c (c/client host port)

               request (s/put! c {:header {:api-key :metadata
                                           :api-version 0
                                           :correlation-id 1
                                           :client-id "greta"}
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

                 "ERROR: Partition not found")

             (str "ERROR: Kafka error: "
                  (name
                   (get-in r [:topics 0 :topic-error-code]))))))

       (fn [r]
         (.close c)
         (d/success-deferred r)))))
