(ns liberator-tutorial.core-test
  (:require [clojure.test :refer :all]
            [ring.mock.request]
            [liberator-tutorial.core :refer :all]
            [liberator-tutorial.db :as db]
            [clojure.java.jdbc :as jdbc]))

;; (defn request [method resource app & params]
;;   (app {:request-method method
;;         :uri resource}
;;        :params (first params)))

(def db-test {:classname   "org.sqlite.JDBC"
              :subprotocol "sqlite"
              :subname     "measures_test.db"})

(korma.db/defdb korma-db-test db-test)

(korma.core/defentity measure-entity
  (korma.core/database db-test)
  (korma.core/table :measure))

(defn recreate-test-database []
  (db/drop-measure-table db-test)
  (db/create-measure-table db-test)
  )

(defn initial-setup [f]
  (print "recreate-test-database")
  (recreate-test-database)
  (f))

(use-fixtures :once initial-setup)

(deftest test-insert-measure
  (testing "Insert a new measure"
    (let [reply (-> (ring.mock.request/request :post "/measure")
                    (ring.mock.request/content-type "text/json")
                    (ring.mock.request/body (clojure.data.json/write-str
                                             {:service "Test1"
                                              :key "key1"
                                              :measure 456
                                              :time 10000}))
                    liberator-tutorial.core/app)
          body (:body reply)
          data (clojure.data.json/read-str body)
          rowid (get-in data ["headers" "rowid"])
          _ (print reply)
          _ (print data)
          _ (print "\n rowid = " rowid)]
      (is (= 201 (:status reply)))
      )))

(deftest test-get-measure
  (testing "Retrieve the measure"
    (let [reply (-> (ring.mock.request/request :get "/measure?service=Test1&key=key1")
                    (ring.mock.request/header "Accept" "text/json")
                    liberator-tutorial.core/app)]
      (is (= 200 (:status reply)))
      )))

