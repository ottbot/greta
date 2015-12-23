(ns greta.producer
  (:require [clojure.tools.logging :as log]
            [greta.core :as c]
            [greta.serde :as serde]
            [manifold.deferred :as d]
            [manifold.stream :as s]))


;; Kinds of partitionining to support:
;; - constant
;; - round-robin around parition count
;; - key based modulo around partition count

;; This protocol is WIP
(defprotocol Partitioner
  (partition-id [this m]))

(defn constant-partitioner [n]
  (reify
    Partitioner
    (partition-id [_ _] n)))


(defn leader-pool
  "Given a bootstrap broker address and a topic, returns a map of
  connections for each partition leader"

  [host port topic serde]

  (d/loop [attempt 1
           connection (c/client host port)]

    (d/chain

     connection

     (fn [conn]
       (c/metadata conn topic))

     (fn [m]
       (let [topic-meta (get-in m [:topics 0])
             brokers (get-in m [:brokers])]

         (if (= :none (:topic-error-code topic-meta))

           [(map #(c/client (:host %)
                            (:port %)
                            serde)
                 (sort-by :node-id brokers))

            (sort-by :partition-id
                     (:partition-metadata topic-meta))]

           (throw (Exception. "Topic error:"
                              (str (:topic-error-code topic-meta)))))))

     (fn [[brokers partitions]]

       (let [errors (map :partition-error-code partitions)
             leaders (map :leader partitions)]

         (cond
           (not-every? #(= :none %) errors)
           (throw (Exception. "Partition errors" (str errors)))

           (some #(= -1 %) leaders)
           (if (> attempt 10)
             (throw (Exception. "Rebalance is taking too long."))
             (do
               (log/info "Brokers are rebalancing, attempt" attempt)
               (Thread/sleep 200)
               (d/recur (inc attempt)
                        connection)))

           :else
           (do
             (.close @connection)
             (mapv #(nth brokers %) leaders))))))))




(defn stream
  "A producer stream. Put messages on the resulting stream."

  [host port topic & {:keys [partitioner
                             serde
                             keyfn
                             acks
                             timeout]

                      :or {partitioner (constant-partitioner 0)
                           serde (serde/string-serde)
                           keyfn nil
                           acks 1
                           timeout 1000}}]



  (let [pool @(leader-pool host port topic serde)
        topic (name topic)
        messages (s/stream)
        results (s/stream)]

    (s/consume

     (fn [m]
       (s/put!
        (nth pool (get-in m
                          [:produce 0 :messages 0 :partition]))
        m))

     (s/map
      (fn [v]
        (->> {:magic-byte :zero
              :attributes :none
              :key (when keyfn (keyfn v))
              :value v}

             (assoc {:offset 0} :message)
             (conj [])

             (assoc {:partition (partition-id partitioner v)}
                    :message-set)
             (conj [])

             (assoc {:topic topic} :messages)
             (conj [])

             (assoc {:required-acks acks
                     :timeout timeout} :produce)

             (conj {:header {:api-key :produce
                             :api-version 0
                             :correlation-id 0
                             :client-id c/client-name}})))
      messages))


    (doseq [x pool]
      (s/connect x results))

    (s/splice
     messages
     (s/map
      #(-> %
           (get-in [0 :results 0])
           (assoc :topic topic))
      results))))
