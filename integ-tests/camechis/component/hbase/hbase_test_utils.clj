(ns camechis.component.hbase.hbase-test-utils
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as lg]
            [camechis.component.hbase.hbase-component :as hbase]
            [clojure.edn :as edn ])
  (:import [org.apache.hadoop.hbase HBaseConfiguration
            HColumnDescriptor
            HTableDescriptor
            TableName]
           [org.apache.hadoop.hbase.client ConnectionFactory]))

(defn put-data [conn tbl dcol ]
  (doseq [d dcol]
    (hbase/put {:connection conn} tbl (:rk d ) (:cf d ) (dissoc d :rk :cf))))


(defn seed-data [conn]
  (let [data (edn/read-string (slurp "resources/seed-data.edn"))
        tables (:tables data)]
    (loop [tbls tables]
      (if-not (seq tbls)
        nil
        (do
          (hbase/create-table {:connection conn} {:name (:table (first tbls))
                                                 :cfs (:cf (first tbls)) })
          (put-data conn (:table (first tbls)) (:data (first tbls)))
          (recur (rest tbls)))))))

(defn delete-all [conn]
  (let [admin (.getAdmin conn)
        tbls (.listTables admin)
        ]
    (doseq [x tbls]
      (hbase/delete-table {:connection conn} (.getTableName x )))))




(defrecord TestHBase [zkquorum zkport connection]
  component/Lifecycle
  (start [component ]
    (let [conn (ConnectionFactory/createConnection
                       (doto (HBaseConfiguration/create )
                         (.set "hbase.zookeeper.quorum" zkquorum)
                         (.set "hbase.zookeeper.property.clientPort" zkport)))]
      (seed-data conn )
      (assoc component :connection conn)))
  (stop [component ]
    (delete-all connection)
    (.close connection)
    (assoc component :connection nil)))


(defn new-test-hbase [zkquorum zkport]
  (map->TestHBase {:zkquorum zkquorum :zkport zkport}))
