(ns greta.codecs.primitives
  (:require [gloss.core :as c])
  (:refer-clojure :exclude [bytes string]))

(c/defcodec string
  (c/finite-frame :int16
                (c/string :utf-8)))

(c/defcodec bytes
  (c/repeated :byte :prefix :int32))

(c/defcodec api-key
  (c/enum :int16 {:produce 0
                :fetch 1
                :offset 2
                :metadata 3
                :offset-commit 8
                :offset-fetch 9
                :group-coordinator 10}))

(c/defcodec magic-byte
  (c/enum :byte {:zero 0}))

(c/defcodec compression
  (c/enum :byte {:none 0
               :gzip 1
               :snappy 2}))

(c/defcodec error
  (c/enum :int16 {:none 0
                :unknown -1
                :offset-out-of-range 1
                :invalid-message 2
                :unknown-topic-or-partition 3
                :invalid-message-size 4
                :leader-not-available 5
                :not-leader-for-partition 6
                :request-timed-out 7
                :broker-not-available 8
                :replica-not-available 9
                :message-size-too-large 10
                :stale-controller-epoch 11
                :offset-metadata-too-large 12
                :offset-load-in-progress 14
                :consumer-coordinator-not-available 15
                :not-coordinator-for-consumer-code 16
                :invalid-topic 17
                :record-list-too-largee	18
                :not-enough-replicas 19
                :not-enough-replicas-after-append	20
                :invalid-required-acks 21
                :illegal-generation	22
                :inconsistent-group-protocol	23
                :invalid-group-id	24
                :unknown-member-id 25
                :invalid-session-timeout 26
                :rebalance-in-progress 27
                :invalid-commit-offset-size	28
                :topic-authorization-failed	29
                :group-authorization-failed	30
                :cluster-authorization-failed	31}))
