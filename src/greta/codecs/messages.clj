(ns  greta.codecs.messages
  (:require [gloss.core.protocols :refer [Reader
                                          Writer
                                          read-bytes]]
            [gloss.core :refer :all]
            [greta.codecs.primitives :as p]
            [greta.serde :refer [serialize
                                 deserialize]]))


(defn message-set [serde]
  (compile-frame
   (ordered-map
    :offset :int64
    :size :int32
    :crc :uint32
    :magic-byte p/magic-byte
    :attributes p/compression
    :key p/bytes
    :value p/bytes)
   (partial serialize serde)
   (partial deserialize serde)))


(defn unterminated-message-set
  "Only a bit nasty. TODO: describe this"
  ([serde max-bytes]
   (unterminated-message-set serde max-bytes []))

  ([serde max-bytes messages]
   (let [codec (message-set serde)
         codec-header (compile-frame [:int64 :int32])
         read-codec-header #(read-bytes codec-header %)
         hsize (sizeof codec-header)]

     (reify
       Reader
       (read-bytes [_ buf-seq]

         (loop [messages messages
                bs buf-seq
                tot 0]

           (let [[success msg r] (read-bytes codec bs)]

             (if success

               (recur (conj messages msg)
                      r
                      (+ tot hsize (:size msg)))

               (if (< tot max-bytes)

                 (let [[success h _] (read-codec-header bs)
                       limit (- max-bytes tot)]

                   (if (or
                        (< hsize limit)
                        (and success (> limit (+ hsize (second h)))))

                     [false (unterminated-message-set limit messages) bs]
                     [true messages nil]))

                 [true messages nil])))))
       Writer
       (sizeof [_]
         nil)
       (write-bytes [_ buf vs]
         (throw (Exception. "Write not implmeneted.")))))))


(defn optimized [serde]
  (compile-frame
   (ordered-map :partition :int32
                :error-code p/error
                :highwater-mark-offset :int64
                :message-set (header :int32
                                     (partial unterminated-message-set serde)
                                     identity))))
