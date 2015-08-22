(ns camechis.component.hbase.hbase-component
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as lg])
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

(defn to-bytes [x]
  (Bytes/toBytes x))

(defn- ^Put make-put [rk]
  (Put. (to-bytes rk)))

(defn- ^Get make-get [rk]
  (Get. (to-bytes rk)))


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
  "create table with the given name/column families"
  [hbase tbl ]
  (try
    (let [adm (.getAdmin (:connection hbase))
          tbldesc (HTableDescriptor. (TableName/valueOf (:name tbl)))]
      (doseq [cf (:cfs tbl) ]
        (.addFamily tbldesc (HColumnDescriptor. cf)))
      (.createTable adm tbldesc ))
    true
    (catch Exception ex (lg/error ex "An error occured while creating table"))))


(defn put [hbase tbl rk cf data ]
  (let [pobj (make-put rk)
        conn (:connection hbase)
        htbl (.getTable conn (TableName/valueOf tbl) )]
    (doseq [ d data ]
      (.add pobj (to-bytes cf) (to-bytes (name (first d))) (to-bytes (second d))))
    (.put htbl pobj)))

(defn hget [hbase tbl rk cf columns]
  (let [htbl (.getTable  (:connection hbase)  (TableName/valueOf tbl))
        gobj (make-get rk)
        resobj (.get htbl gobj ) ]
    (into {}  (map (fn [x]
                     {x (. Bytes toString (.getValue resobj (to-bytes cf)
                                                     (to-bytes (name x)) ))})
                   columns ))))
