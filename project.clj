(defproject camechis/hbase-component "0.1.0"
  :description "A clojure application to do data loads using Spark"
  :url "https://github.com/camechis/hbase-component"
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-log4j12 "1.7.12"]
                 [com.stuartsierra/component "0.2.3"]
                 [org.apache.hbase/hbase-client "1.1.1"]]
  :aot :all
  :test-paths ["test" "integration-test"]

  :test-selectors {:default (complement (some-fn :integration))
                   :integration :integration
                   :all (fn [m] true)}
  :javac-options ["-Xlint:unchecked" "-source" "1.7" "-target" "1.7"]
  :jvm-opts ["-server" "-Xmx2g" "-Dsun.io.serialization.extendedDebugInfo=true"]
  :global-vars {*warn-on-reflection* true}

  :plugins [[lein-kibit "0.0.8"]
            [lein-shell "0.4.1"]
            [lein-auto "0.1.2"]
            [lein-pprint "1.1.1"]]

  :profiles { :uberjar {:aot :all}
             :dev {:resource-paths ["test/resources"]
                   :dependencies [[org.clojure/tools.nrepl "0.2.7"]]}})
