(ns camechis.component.hbase.hbase-component
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as lg]
            [camechis.component.hbase.byte_utils :as bu])
  (:import [org.apache.hadoop.hbase HBaseConfiguration
            HColumnDescriptor
            HTableDescriptor
            TableName]
           [org.apache.hadoop.hbase.client ConnectionFactory Put Get]
           [org.apache.hadoop.hbase.util Bytes]))

(defrecord HBase [zkquorum zkport connection]
  component/Lifecycle
  (start [component ]
    (if connection
      component
      (assoc component :connection
             (ConnectionFactory/createConnection
              (doto (HBaseConfiguration/create )
                (.set "hbase.zookeeper.quorum" zkquorum)
                (.set "hbase.zookeeper.property.clientPort" zkport))))))
  (stop [component ]
    (if-not connection
      component
      (do (.close  connection)
          (assoc component :connection nil)))))

(defn- ^Put make-put [rk]
  (Put. (bu/to-bytes rk)))

(defn- ^Get make-get [rk]
  (Get. (bu/to-bytes rk)))


(defn new-hbase [zkquorum zkport]
  (map->HBase {:zkquorum zkquorum :zkport zkport}))

(defn delete-table
  "delete table with name"
  [hbase tbl]
  (try
    (doto (.getAdmin (:connection hbase))
      (.disableTable tbl)
      (.deleteTable tbl))
    true
    (catch Exception ex (lg/error ex "An error occured while deleting table"))))

(defn create-table
  "create table with the given name/column families
  tbl = {:name val :cfs [vals]}"
  [hbase tbl ]
  (try
    (let [adm (.getAdmin (:connection hbase))
          tbldesc (HTableDescriptor. (TableName/valueOf (:name tbl)))]
      (doseq [cf (:cfs tbl) ]
        (.addFamily tbldesc (HColumnDescriptor. cf)))
      (.createTable adm tbldesc ))
    true
    (catch Exception ex (lg/error ex "An error occured while creating table"))))


(defn put
  "add data to cf:table with the given rowkey
  data = [[col val] [col val]]"
  [hbase tbl rk cf data ]
  (let [pobj (make-put rk)
        conn (:connection hbase)
        htbl (.getTable conn (TableName/valueOf tbl) )]
    (doseq [ d data ]
      (.add pobj (bu/to-bytes cf) (bu/to-bytes (name (first d))) (bu/to-bytes (second d))))
    (.put htbl pobj)))

(defn hget
  "get a row with give rowkey, columns is a vector of column names,
  return is a map of colum names to values"
  [hbase tbl rk cf columns]
  (let [htbl (.getTable  (:connection hbase)  (TableName/valueOf tbl))
        gobj (make-get rk)
        resobj (.get htbl gobj ) ]
    (into {}  (map (fn [x]
                     {x (Bytes/toString (.getValue resobj (bu/to-bytes cf)
                                                     (bu/to-bytes (name x)) ))})
                   columns ))))
