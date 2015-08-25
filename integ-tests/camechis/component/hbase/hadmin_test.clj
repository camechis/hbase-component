(ns  ^:integration camechis.component.hbase.hadmin-test
  (:require [com.stuartsierra.component :as component]
            [camechis.component.hbase.hbase-component :as hbase]
            [clojure.tools.logging :as lg])
  (:use [clojure.test]
        [clojure.test.junit]))


(def table-name "testtable" )

(def hbase (atom nil) )

(defn teardown-hbase-tbl [tbl]
  (try
    (doto (.getAdmin (:connection @hbase))
      (.disableTable tbl)
      (.deleteTable tbl))
    true
    (catch Exception ex )))


(defn clean-db-fixture [f]
  (f)
  (teardown-hbase-tbl table-name))

(defn startup-shutdown-hbase [f]
  (reset! hbase  (component/start (hbase/new-hbase "hbase" "2181")))
  (f)
  (component/stop @hbase))

(use-fixtures :each clean-db-fixture)
(use-fixtures :once startup-shutdown-hbase )


(deftest create-hbase-table
  (is (= true (hbase/create-table @hbase {:name table-name :cfs ["foo" "bar"]}))
      "create hbase table with families"))

(deftest delete-hbase-table
  (hbase/create-table @hbase {:name table-name :cfs ["foo"]})
  (is (= true (hbase/delete-table @hbase table-name))
      "delete hbase table"))
