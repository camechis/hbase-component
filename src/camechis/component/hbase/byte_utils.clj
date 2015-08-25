(ns camechis.component.hbase.byte_utils
  (:import [org.apache.hadoop.hbase.util Bytes]))


(defn to-bytes [x]
  (Bytes/toBytes x))

(defmulti from-bytes :type )

(defmethod from-bytes :string
  [x]
  (Bytes/toString (:bytes x)))

(defmethod from-bytes :int
  [x]
  (Bytes/toInt (:bytes x)))

(defmethod from-bytes :float
  [x]
  (Bytes/toFloat (:bytes x)))

(defmethod from-bytes :double
  [x]
  (Bytes/toDouble (:bytes x)))


(defmethod from-bytes :long
  [x]
  (Bytes/toLong (:bytes x)))
