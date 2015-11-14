(ns greta.codecs.core-test
  (:require [clojure.test :refer :all]
            [gloss.io :as io]
            [greta.codecs.core :refer :all]))


(defn round-trip?
  ([e m] (round-trip? e e m))
  ([e d m]
   (= m (->> m
             (io/encode e)
             (io/decode d)))))
