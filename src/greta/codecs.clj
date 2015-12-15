(ns greta.codecs
  (:require [gloss.core :refer :all]
            [greta.codecs.messages :as messages]
            [greta.codecs.primitives :as p]
            [manifold.stream :as s]))


(defprotocol KafkaApi
  "A codec used to encode requests and decode responses sent
  and received from Kafka."
  (request [t])
  (response [t]))



(defmacro defapi
  "Defines an..."
  [title signature request-frame response-frame]

  `(defn ~title ~signature
     (reify
       KafkaApi
       (request [_]
         (compile-frame ~request-frame))
       (response [_]
         (compile-frame ~response-frame)))))


(defapi metadata []

  (ordered-map
   :topics (repeated p/string))

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
                                   :isr (repeated :int32)))))))


(defapi offset []

  (ordered-map
   :replica-id :int32
   :topics (repeated
            (ordered-map
             :topic p/string
             :partitions (repeated
                          (ordered-map
                           :partition :int32
                           :time :int64
                           :max-number-of-offsets :int32)))))

  (repeated
   (ordered-map
    :topic p/string
    :partitions (repeated
                 (ordered-map
                  :partition :int32
                  :error-code p/error
                  :offsets (repeated :int64))))))


(defapi fetch [serde]

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
                         :max-bytes :int32)))))

  (repeated
   (ordered-map
    :topic-name p/string
    :messages (repeated
               (compile-frame
                (ordered-map :partition :int32
                             :error-code p/error
                             :highwater-mark-offset :int64
                             :message-set (messages/message-set serde)))))))



(defapi produce [serde]

  (ordered-map
   :required-acks :int16
   :timeout :int32
   :produce (repeated
             (ordered-map
              :topic p/string
              :messages (repeated
                         (ordered-map
                          :partition :int32
                          :message-set (messages/message-set serde))))))

  (repeated
   (ordered-map
    :topic p/string
    :results (repeated
              (ordered-map
               :partition :int32
               :error-code p/error
               :offset :int64)))))


(defapi offset-fetch []

  (ordered-map
   :consumer-group p/string
   :topics (repeated
            (ordered-map
             :topic p/string
             :partitions (repeated :int32))))

  (repeated
   (ordered-map
    :topic p/string
    :partitions (repeated
                 (ordered-map
                  :partition-id :int32
                  :offset :int64
                  :metadata p/string
                  :error-code p/error)))))


(defapi offset-commit []

  (ordered-map
   :consumer-group-id p/string
   :consumer-group-generation-id :int32
   :consumer-id p/string
   :retention-time :int64
   :topics (repeated
            (ordered-map
             :topic p/string
             :partitions (repeated
                          (ordered-map
                           :partition-id :int32
                           :offset :int64
                           :metadata p/string)))))

  (repeated
   (ordered-map
    :topic p/string
    :partitions (repeated
                 (ordered-map
                  :partition-id :int32
                  :error-code p/error)))))


(defapi group-coordinator []

  (ordered-map
   :consumer-group p/string)

  (ordered-map
   :error-code p/error
   :coordinator-id :int32
   :host p/string
   :port :int32))


(defapi join-group []

  (ordered-map
   :group-id p/string
   :session-timeout :int32
   :member-id p/string
   :protocol-type p/string
   :group-protocols (repeated
                     (ordered-map
                      :protocol-name p/string
                      :protocol-metadata (finite-frame
                                          :int32
                                          (ordered-map
                                           :version :int16
                                           :subscription (repeated p/string)
                                           :user-data p/bytes)))))

  (ordered-map
   :error-code p/error
   :generation-id :int32
   :group-protocol p/string
   :leader-id p/string
   :member-id p/string
   :members (repeated
             (ordered-map
              :id p/string
              :metadata p/bytes))))


(let [member-assignment (finite-frame
                         :int32
                         (ordered-map
                          :version :int16
                          :partition-assignment (repeated
                                                 (ordered-map
                                                  :topic p/string
                                                  :partitions (repeated :int32)))
                          :user-data p/bytes))]

  (defapi sync-group []

    (ordered-map
     :group-id p/string
     :generation-id :int32
     :member-id p/string
     :group-assignment (repeated
                        (ordered-map
                         :member-id p/string
                         :member-assignment member-assignment)))


    (ordered-map
     :error-code p/error
     :member-assignment member-assignment)))


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
              :produce (produce serde)
              :join-group (join-group)
              }]

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
