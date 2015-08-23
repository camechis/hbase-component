(ns camechis.component.hbase.byte_utils
  (:import [org.apache.hadoop.hbase.util Bytes]))


(defn to-bytes [x]
  (Bytes/toBytes x))
