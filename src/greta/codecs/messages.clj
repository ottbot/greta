(ns  greta.codecs.messages
  (:require [gloss.core.protocols :refer [Reader
                                          Writer
                                          read-bytes]]
            [gloss.core :refer :all]
            [gloss.data.bytes.core :as b]
            [greta.codecs.primitives :as p]
            [greta.serde :refer [serialize
                                 deserialize]]))


(defn message
  "A kafka message. This may be smaller than the given size, the
  remaining bytes wil be discarded."
  [serde size]
  (let [message-frame (compile-frame
                       (ordered-map
                        :crc :int32
                        :magic-byte p/magic-byte
                        :attributes p/compression
                        :key p/bytes
                        :value p/bytes)
                       (partial serialize serde)
                       (partial deserialize serde))]

    (reify
      Reader
      (read-bytes [_ buf-seq]

        (let [[s x r] (read-bytes message-frame
                                  (b/take-bytes buf-seq size))]
          (if s
            [true x (b/drop-bytes buf-seq size)]
            [false x r])))

      Writer
      (sizeof [_] nil)
      (write-bytes [_ buf vs]
        (throw (Exception. "Write not implmeneted."))))))


(defn message-set
  "Builds a list of "
  ([serde max-bytes]
   (message-set serde max-bytes []))

  ([serde max-bytes messages]

   (let [message-set-frame (compile-frame
                            (ordered-map
                             :offset :int64
                             :message (header
                                       :int32
                                       (partial message serde)
                                       identity)))]

     (reify
       Reader
       (read-bytes [_ buf-seq]

         (loop [ms messages
                bs buf-seq
                limit max-bytes]

           (let [[success x r] (read-bytes message-set-frame bs)]

             (if success

               (recur
                (conj ms x) r (- limit (byte-count bs)))

               (if (< (byte-count r) limit)
                 [false (message-set serde limit messages) bs]
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
                                     (partial message-set serde)
                                     identity))))
