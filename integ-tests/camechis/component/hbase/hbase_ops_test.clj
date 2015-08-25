(ns  ^:integration camechis.component.hbase.hbase-ops-test
  (:require [com.stuartsierra.component :as component]
            [camechis.component.hbase.hbase-component :as hbase]
            [clojure.tools.logging :as lg])
  (:use [clojure.test]
        [clojure.test.junit])
  (:import [org.apache.hadoop.hbase  TableNotFoundException]))


(def hbase (atom nil) )

(def ttable "testtable")

(defn startup-shutdown-hbase [f]
  (reset! hbase  (component/start (hbase/new-hbase "hbase" "2181")))
  (hbase/create-table @hbase {:name ttable :cfs ["A"]} )
  (f)
  (hbase/delete-table @hbase ttable)
  (component/stop @hbase))

(use-fixtures :once startup-shutdown-hbase )

(deftest test-put []
  (let [data {:c1 "foo" :c2 "bar"}]
    (is (= nil (hbase/put @hbase ttable "1" "A" data)))))

(deftest test-put-nonexistent-table []
  (is (thrown? Exception (hbase/put @hbase "foo" "1" "A"  {:c1 "foo" :c2 "bar"} ))))

(deftest test-hget
  (hbase/put @hbase ttable "1" "A" {:c1 "foo" :c2 "bar"})
  (is (= {:c1 "foo"} (hbase/hget @hbase ttable "1" "A" [:c1] ))))
