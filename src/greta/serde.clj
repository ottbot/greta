(ns greta.serde)

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
  (reify
    KafkaSerde
    (serialize [_ m]
      (letfn [(xf [v] (-> v
                          .getBytes
                          bytes
                          vec))]
        (-> m
            (update :key xf)
            (update :value xf))))

    (deserialize [_ m]
      (letfn [(xf [v] (if (= (first v) -1)
                        nil
                        (transduce (map char) str v)))]
        (-> m
            (update :key xf)
            (update :value xf))))))
