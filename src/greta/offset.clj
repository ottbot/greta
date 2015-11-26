(ns greta.offset
  (:require [greta.codecs.group-coordinator :as gc]
            [greta.codecs.offset-commit :as oc]
            [greta.codecs.offset-fetch :as of]
            [greta.core :as c]
            [manifold.deferred :as d]
            [manifold.stream :as s]))

(defn coordinator
  "Finds the host and port for the consumer offset manager."
  [host port consumer-group]

  (d/let-flow [c (c/client host
                           port
                           gc/request
                           gc/response)

               req (s/put! c {:api-key :group-coordinator
                              :api-version 0
                              :correlation-id 1
                              :client-id "greta"
                              :consumer-group consumer-group})]

      (d/chain
       (s/take! c ::drained)

       (fn [r]
         (if (= r ::drained)
           (throw (Exception. "connection closed"))

           (select-keys r [:error-code
                           :host
                           :port
                           :coordinator-id])))

       (fn [r]
         (.close c)
         (d/success-deferred r)))))




(defn offset-committer [host port consumer-group]
  (let [{:keys [host port error-code]}
        @(coordinator host port consumer-group)]

    (if (= error-code :none)
      (c/client host port oc/request oc/response)
      error-code)))


(defn offset-fetcher [host port consumer-group]
  (let [{:keys [host port error-code]}
        @(coordinator host port consumer-group)]

    (if (= error-code :none)
      (c/client host port of/request of/response)
      error-code)))
