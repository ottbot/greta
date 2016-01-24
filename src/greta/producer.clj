(ns greta.producer
  (:require [clojure.tools.logging :as log]
            [greta.core :as c]
            [greta.serde :as serde]
            [manifold.deferred :as d]
            [manifold.stream :as s]))

(defprotocol Partitioner
  "Used to define strategies to define which parition a message should
  be places. "
  (next-id [this connections message]))

(defn constant-partitioner [x]
  (reify
    Partitioner
    (next-id [_ _ _] x)))

(defn random-parititioner []
  (reify
    Partitioner
    (next-id [_ _ conns]
      (rand-int (count conns)))))

(defn round-robin-partitioner []
  (let [prev (atom -1)]
    (reify
      Partitioner
      (next-id [_ _ conns]
        (swap! prev #(mod (inc %)
                          (count conns)))))))



(defn leader-connections
  "Given a bootstrap broker address and a topic, returns a map of
  connections for each partition leader"
  ([host port topic serde]
   (leader-connections (c/client host port) topic serde))

  ([bootstrap-connection topic serde]

   (d/loop [attempt 1
            connection bootstrap-connection]

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

            (throw (ex-info "Topic error"
                            {:topic-metadata topic-meta})))))

      (fn [[brokers partitions]]

        (let [errors (map :partition-error-code partitions)
              leaders (map :leader partitions)]

          (cond
            (not-every? #(= :none %) errors)
            (throw (ex-info "Partition error"
                            {:partition-metadata partitions}))

            (some #(= -1 %) leaders)
            (if (> attempt 10)
              (throw (ex-info "Rebalance timeout"
                              {:partition-metadata partitions}))
              (do
                (log/info "Brokers are rebalancing, attempt" attempt)
                (Thread/sleep 200)
                (d/recur (inc attempt)
                         connection)))

            :else
            (do
              (.close @connection)
              (mapv #(nth brokers %) leaders)))))))))

(defn key-base-partitioner []
  (throw (Exception. "not implemented")))

(defn stream
  "A producer stream. Put messages on the resulting stream."

  [host port topic & {:keys [partitioner
                             serde
                             keyfn
                             acks
                             timeout]

                      :or {partitioner (round-robin-partitioner)
                           serde (serde/string-serde)
                           keyfn nil
                           acks 1
                           timeout 1000}}]



  (let [pool @(leader-connections host port topic serde)
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

             (assoc {:partition (next-id partitioner v pool)}
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
