(ns greta.codecs.fetch
  (:require [gloss.core :refer :all]
            [gloss.core.protocols :as p]
            [greta.codecs.core :as c]))

(defcodec fetch-topic
  (ordered-map
   :topic c/sized-string
   :messages (repeated
              (ordered-map
               :partition :int32
               :fetch-offset :int64
               :max-bytes :int32))))


(defcodec request
  (finite-frame :int32
   (ordered-map
    :api-key c/api-key
    :api-version :int16
    :correlation-id :int32
    :client-id c/sized-string
    :replica-id :int32
    :max-wait-time :int32
    :min-bytes :int32
    :topics (repeated fetch-topic))))


(defn message-set [serde]
  (compile-frame
   (ordered-map
    :offset :int64
    :size :int32
    :crc :uint32
    :magic-byte c/magic-byte
    :attributes c/compression
    :key c/sized-bytes
    :value c/sized-bytes)
   (partial c/serialize serde)
   (partial c/deserialize serde)))



(defn unterminated-message-set
  "Only a bit nasty"
  ([serde max-bytes]
   (unterminated-message-set serde max-bytes []))

  ([serde max-bytes messages]
   (let [codec (message-set serde)
         codec-header (compile-frame [:int64 :int32])
         read-codec-header #(p/read-bytes codec-header %)
         hsize (sizeof codec-header)]

     (reify
       p/Reader
       (read-bytes [_ buf-seq]

         (loop [messages messages
                bs buf-seq
                tot 0]

           (let [[success msg r] (p/read-bytes codec bs)]

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
       p/Writer
       (sizeof [_]
         nil)
       (write-bytes [_ buf vs]
         (throw (Exception. "Write not implmeneted.")))))))


(defn optimized-messages [s]
  (compile-frame
   (ordered-map :partition :int32
                :error-code :int16
                :highwater-mark-offset :int64
                :message-set (header :int32
                                     (partial unterminated-message-set s)
                                     identity))))


(defn response [serde]
  (compile-frame
   (finite-frame
    :int32
    (ordered-map
     :correlation-id :int32
     :topics (repeated
              (ordered-map
               :topic-name c/sized-string
               :messages (repeated (optimized-messages serde))))))))



;; TODO: Implement Writer for unterminated-message-set, then we don't
;; need these for testing!

(defn fixed-size-messages [s]
  (compile-frame
   (ordered-map
    :partition :int32
    :error-code :int16
    :highwater-mark-offset :int64
    :message-set (finite-frame :int32
                               (repeated (message-set s)
                                         :prefix :none)))))


(defn fixed-size-response [s]
  (compile-frame
   (finite-frame
    :int32
    (ordered-map
     :correlation-id :int32
     :topics (repeated
              (ordered-map
               :topic-name c/sized-string
               :messages (repeated (fixed-size-messages s))))))))
