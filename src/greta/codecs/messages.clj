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
                                 deserialize]]))

(defn- write-finite-bytes [prefix codec v]
  (b/create-buf-seq
   (write-bytes
    (finite-frame prefix codec)
    nil v)))

(defn message
  "A kafka message. This may be smaller than the given size, the
  remaining bytes wil be discarded."
  [serde]
  (let [prefix (compile-frame :int32)

        codec (compile-frame
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

        (read-bytes
         (compose-callback
          prefix

          (fn [size b]
            (let [[s x r] (read-bytes codec
                                      (b/take-bytes b size))]
              (if s
                [true x (b/drop-bytes b size)]
                [false x r]))))

         buf-seq))

      Writer
      (sizeof [_] nil)
      (write-bytes [_ _ v]
        (write-finite-bytes prefix codec v)))))


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
         (write-finite-bytes prefix
                             (repeated codec :prefix :none) v))))))
