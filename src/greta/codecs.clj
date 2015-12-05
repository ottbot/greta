(ns greta.codecs
  (:require [gloss.core :refer :all]
            [greta.codecs.messages :as messages]
            [greta.codecs.primitives :as p]
            [greta.codecs.produce :as produce-codec]
            [manifold.stream :as s]))


(defprotocol KafkaApi
  "Provides a codec used to encode requests and decode responses sent
  and received from Kafka."
  (request [t])
  (response [t]))


(defn metadata []
  (reify
    KafkaApi

    (request [_]
      (compile-frame
       (ordered-map
        :topics (repeated p/string))))

    (response [_]
      (compile-frame
       (ordered-map
        :brokers (repeated
                  (ordered-map
                   :node-id :int32
                   :host p/string
                   :port :int32))

        :topics (repeated
                 (ordered-map
                  :topic-error-code p/error
                  :topic-name p/string
                  :partition-metadata (repeated
                                       (ordered-map
                                        :partition-error-code p/error
                                        :partition-id :int32
                                        :leader :int32
                                        :replicas (repeated :int32)
                                        :isr (repeated :int32))))))))))


(defn offset []
  (reify
    KafkaApi

    (request [_]
      (compile-frame
       (ordered-map
        :replica-id :int32
        :topics (repeated
                 (ordered-map
                  :topic p/string
                  :partitions (repeated
                               (ordered-map
                                :partition :int32
                                :time :int64
                                :max-number-of-offsets :int32)))))))

    (response [_]
      (compile-frame
       (repeated
        (ordered-map
         :topic p/string
         :partitions (repeated
                      (ordered-map
                       :partition :int32
                       :error-code p/error
                       :offsets (repeated :int64)))))))))


(defn fetch [serde]
  (reify
    KafkaApi

    (request [_]
      (compile-frame
       (ordered-map
        :replica-id :int32
        :max-wait-time :int32
        :min-bytes :int32
        :topics (repeated
                 (ordered-map
                  :topic p/string
                  :messages (repeated
                             (ordered-map
                              :partition :int32
                              :fetch-offset :int64
                              :max-bytes :int32)))))))

    (response [_]
      (compile-frame
       (ordered-map
        :topics (repeated
                 (ordered-map
                  :topic-name p/string
                  :messages (repeated
                             (compile-frame
                              (ordered-map :partition :int32
                                           :error-code p/error
                                           :highwater-mark-offset :int64
                                           :message-set (messages/message-set serde)))))))))))


;; REFACTOR produce should use the message ns
(defn produce [serde]
  (reify
    KafkaApi
    (request [_]
      (compile-frame
       (produce-codec/request serde)))
    (response [_]
      produce-codec/response)))


(defn offset-fetch []
  (reify
    KafkaApi

    (request [_]
      (compile-frame
       (ordered-map
        :consumer-group p/string
        :topics (repeated
                 (ordered-map
                  :topic p/string
                  :partitions (repeated :int32))))))

    (response [_]
      (compile-frame
       (ordered-map
        :topics (repeated
                 (ordered-map
                  :topic p/string
                  :partitions (repeated
                               (ordered-map
                                :partition-id :int32
                                :offset :int64
                                :metadata p/string
                                :error-code p/error)))))))))


(defn offset-commit []
  (reify
    KafkaApi

    (request [_]
      (compile-frame
       (ordered-map
        :consumer-group-id p/string
        :consumer-group-generation-id :int32
        :consumer-id p/string
        :topics (repeated
                 (ordered-map
                  :topic p/string
                  :partitions (repeated
                               (ordered-map
                                :partition-id :int32
                                :offset :int64
                                :timestamp :int64
                                :metadata p/string)))))))

    (response [_]
      (compile-frame
       (ordered-map
        :topics (repeated
                 (ordered-map
                  :topic p/string
                  :partitions (repeated
                               (ordered-map
                                :partition-id :int32
                                :error-code p/error)))))))))


(defn group-coordinator []
  (reify
    KafkaApi

    (request [_]
      (compile-frame
       (ordered-map
        :consumer-group p/string)))

    (response [_]
      (compile-frame
       (ordered-map
        :error-code p/error
        :coordinator-id :int32
        :host p/string
        :port :int32)))))




(defn correlated-codecs
  "This ensures that responses are returned in order.

  Note: you can only have up to 1000 pending requests until new
  requests are blocked. Would you like to have this number be configurable?
  "
  [serde]
  (let [requests (s/stream 1000)

        request-count (atom 0)

        next-correlation-id! (fn [_]
                               (swap! request-count
                                      #(mod (inc %) Integer/MAX_VALUE)))

        request-header (compile-frame
                        (ordered-map
                         :api-key p/api-key
                         :api-version :int16
                         :correlation-id :int32
                         :client-id p/string))

        apis {:metadata (metadata)
              :offset (offset)
              :offset-commit (offset-commit)
              :offset-fetch (offset-fetch)
              :group-coordinator (group-coordinator)
              :fetch (fetch serde)
              :produce (produce serde)}]

    (reify

      KafkaApi

      (request [_]
        (compile-frame
         (finite-frame
          :int32

          (header
           request-header

           (fn [m]
             (let [cid (:correlation-id m)
                   api (-> m :api-key apis)]

               @(s/put! requests {:correlation-id cid
                                  :api api})

               (request api)))

           (fn [m]
             (:header
              (update-in m
                         [:header :correlation-id]
                         next-correlation-id!)))))))

      (response [_]
        (compile-frame
         (finite-frame
          :int32

          (header
           :int32 ;; correlation-id

           (fn [cid]
             (let [m @(s/take! requests)]
               (assert (= cid (:correlation-id m)) "ERROR: correlation id does not match.")
               (response (:api m))))

           :correlation-id)))))))
