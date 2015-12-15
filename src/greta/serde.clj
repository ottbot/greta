(ns greta.serde
  (:require [byte-streams :as bs]))

(defprotocol KafkaSerde
  "Takes a map containing a :key and :value, and returns the same map
  but with (de)serialized values for those keys"
  (serialize [this v])
  (deserialize [this v]))


(defn identity-serde
  "Doesn't modify values."
  []
  (reify
    KafkaSerde
    (serialize [_ m] m)
    (deserialize [_ m] m)))


(defn string-serde
  "Takes and returns strings"
  []
  (letfn [(update-bytes [m xf]
            (-> m
                (update :key #(some-> % xf))
                (update :value #(some-> % xf))))]
    (reify
      KafkaSerde
      (serialize [_ m]
        (update-bytes m bs/to-byte-buffers))

      (deserialize [_ m]
        (update-bytes m bs/to-string)))))
