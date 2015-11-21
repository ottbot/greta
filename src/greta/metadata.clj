(ns greta.metadata
  (:require [greta.codecs.metadata :as mdc]
            [greta.core :as c]))

(defn client [host port]
  (c/client host
            port
            mdc/request
            mdc/response))
