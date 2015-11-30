(ns greta.codecs.messages-test
  (:require [greta.codecs.messages :refer :all]
            [clojure.test :refer :all]))




(def msg {:header {:api-key :fetch
                     :api-version 0
                     :correlation-id 1
                     :client-id "greta-test"}

          :replica-id -1
          :max-wait-time 1000
          :min-bytes 1
          :topics [{:topic "greta-tests"
                    :messages [{:partition 0
                                :fetch-offset 0
                                :max-bytes 10240}]}]})


(def c @(greta.core/client "localhost" 9092 (greta.serde/string-serde)))


@(manifold.stream/put! c msg)


(def res
  @(manifold.stream/try-take! c ::drained 10000 ::timeout))

(def messages (map :message (get-in res [:topics 0 :messages 0 :message-set])))




(defn decode [m] (gloss.io/decode (message) m ))


(def ded (loop [col messages
                msgs []
                n 0]
           (println "message" n)
           (if (empty? (rest col))
             msgs
             (recur (rest col) (conj msgs (decode (first col))) (inc n)))))


(def bad-msg (nth messages 33))

(decode bad-msg)
