(ns  greta.codecs.messages
  (:require [gloss.core.protocols :refer [Reader
                                          Writer
                                          read-bytes
                                          with-buffer
                                          write-bytes
                                          compose-callback]]
            [gloss.core :refer :all]
            [gloss.data.bytes.core :as b]
            [greta.codecs.primitives :as p]
            [greta.serde :refer [serialize
                                 deserialize]])

    (:import [java.nio HeapByteBuffer]))


(defn crc [^HeapByteBuffer x]
  (.getValue
   (doto (java.util.zip.CRC32.)
     (.reset)
     (.update x))))


;; this will look nice eventually.

(defn message
  "A kafka message. This may be smaller than the given size, the
  remaining bytes wil be discarded.

  The codec will compute the required CRC when producing a message
  and verify the incoming CRC when fetching.
  "
  [serde]
  (let [prefix (compile-frame :int32)

        check (compile-frame :uint32)

        codec (compile-frame
               (ordered-map
                :magic-byte p/magic-byte
                :attributes p/compression
                :key p/bytes
                :value p/bytes)
               (partial serialize serde)
               (partial deserialize serde))

        crc* (fn [bs]
               (crc (b/take-contiguous-bytes
                     bs (b/byte-count bs))))]

    (reify
      Reader
      (read-bytes [_ buf-seq]
        (read-bytes
         (compose-callback
          prefix
          (fn [size bs]
            (let [[s x r] (read-bytes
                           (compose-callback
                            check
                            (fn [x bs']
                              ;; (assert (= x (crc* bs')) "CRC matches")
                              (read-bytes codec bs')))
                           bs)]
              (if s
                [true x (b/drop-bytes bs size)]
                [false x r]))))

         buf-seq))

      Writer
      (sizeof [_] nil)
      (write-bytes [_ _ v]
        (let [body (b/create-buf-seq (write-bytes codec nil v))
              size (b/byte-count body)]
          (concat
           (write-bytes prefix nil (+ (sizeof check)
                                      size))
           (write-bytes check nil (crc* body))
           body))))))


(defn message-set
  "Builds a list of messages."
  ([serde]
   (message-set serde []))

  ([serde messages]

   (let [prefix (compile-frame :int32)

         codec (compile-frame
                (ordered-map
                 :offset :int64
                 :message (message serde)))]

     (reify
       Reader
       (read-bytes [_ buf-seq]

         (read-bytes
          (compose-callback
           prefix

           (fn [size bs]
             (loop [ms messages
                    bs bs
                    limit size]

               (let [[success x r] (read-bytes codec bs)]

                 (if success
                   (recur
                    (conj ms x) r (- limit (- (byte-count bs)
                                              (byte-count r))))

                   (if (< 0 (byte-count r) limit)
                     [false (message-set serde ms) bs]
                     [true ms nil]))))))

          buf-seq))

       Writer
       (sizeof [_]
         nil)
       (write-bytes [_ _ v]
         (b/create-buf-seq
          (write-bytes
           (finite-frame prefix
                         (repeated codec :prefix :none))
           nil v)))))))
