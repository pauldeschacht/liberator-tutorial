(ns liberator-tutorial.core-test
  (:require [clojure.test :refer :all]
            [ring.mock.request]
            [liberator-tutorial.core :refer :all]
            [liberator-tutorial.db :as db]
            [clojure.java.jdbc :as jdbc]))

(defn initial-setup [f]
  (print "initial-setup test database\n")
  (with-redefs [db/db {:classname   "org.sqlite.JDBC"
                       :subprotocol "sqlite"
                       :subname     "measures_test.db"}
                db/korma-db (korma.db/create-db {:classname   "org.sqlite.JDBC"
                                                 :subprotocol "sqlite"
                                                 :subname     "measures_test.db"})
                db/measure-entity {:table "measure"
                                   :db {:classname   "org.sqlite.JDBC"
                                        :subprotocol "sqlite"
                                        :subname     "measures_test.db"}
                                   :pk :id
                                   :transforms '()
                                   :prepares '()
                                   :fields []
                                   :rel {}}
                ]
    (korma.db/default-connection db/korma-db)
    (db/drop-measure-table db/db)
    (db/create-measure-table db/db)
    (f)
    (print "mock database done\n")
    )
  )

(use-fixtures :once initial-setup)

(deftest test-insert-and-retrieve-single-measure
  (testing "Insert a new measure"
    (let [reply-insert (-> (ring.mock.request/request :post "/measure")
                           (ring.mock.request/content-type "text/json")
                           (ring.mock.request/body (clojure.data.json/write-str
                                                    {:service "Test-Service-1"
                                                     :key "Test-Key-1"
                                                     :measure 123456789.0
                                                     :time 10000}))
                           liberator-tutorial.core/app)
          rowid (get-in reply-insert [:headers "rowid"])
          ]
      
      (is (= 201 (:status reply-insert)))
      (is (integer? (Integer/parseInt rowid)))
      (let [reply-retrieve (-> (ring.mock.request/request :get (str "/measure/" rowid))
                               (ring.mock.request/header "Accept" "text/json")
                               liberator-tutorial.core/app)
            retrieved (clojure.data.json/read-str (:body reply-retrieve) :key-fn keyword)
            ]
        (is (= 200 (:status reply-retrieve)))
        (is (= 1 (count retrieved)))
        (is (= (Integer/parseInt rowid) (:uid (first retrieved))))
        (is (= "Test-Service-1" (:service (first retrieved))))
        (is (= 123456789.0 (:measure (first retrieved))))
        )
      )))

(deftest test-insert-and-retrieve-multiple-measures
  (testing "Insert several measures at once"
    (let [reply (-> (ring.mock.request/request :post "/measure")
                    (ring.mock.request/content-type "text/json")
                    (ring.mock.request/body (clojure.data.json/write-str
                                             [ {:service "Test-Service-2" :key "Test-Key-2" :measure 0 :time 20000 :tags "simple test 1"}
                                               {:service "Test-Service-2" :key "Test-Key-2" :measure 1 :time 20001 :tags "simple test 2"}
                                               {:service "Test-Service-2" :key "Test-Key-2" :measure 2 :time 20002 :tags "simple test 3"}
                                               {:service "Test-Service-2" :key "Test-Key-2" :measure 3 :time 20003 :tags "simple test 4"}
                                               {:service "Test-Service-2" :key "Test-Key-2" :measure 4 :time 20004 :tags "simple test 5"}
                                               {:service "Test-Service-2" :key "Test-Key-2" :measure 5 :time 20005 :tags "simple test 6"}
                                               {:service "Test-Service-2" :key "Test-Key-2" :measure 6 :time 20006 :tags "simple test 7"}
                                               {:service "Test-Service-2" :key "Test-Key-2" :measure 7 :time 20007 :tags "simple test 8"}
                                               {:service "Test-Service-2" :key "Test-Key-2" :measure 8 :time 20008 :tags "simple test 9"}
                                               {:service "Test-Service-2" :key "Test-Key-2" :measure 9 :time 20009 :tags "simple test 10"}
                                               ]))
                    liberator-tutorial.core/app)]
      (is (= 201 (:status reply)))
      ))
  (testing "Retrieve some measures based on service and key"
    (let [reply (-> (ring.mock.request/request :get "/measure?service=Test-Service-2&key=Test-Key-2")
                    (ring.mock.request/header "Accept" "text/json")
                    liberator-tutorial.core/app)
          retrieved (clojure.data.json/read-str (:body reply) :key-fn keyword)
          ]
      (is (= 200 (:status reply)))
      (is (= 10 (count retrieved)))))

  (testing "Retrieve some measures based on service, key and time"
    (let [reply (-> (ring.mock.request/request :get "/measure?service=Test-Service-2&key=Test-Key-2&from=20005&to=20008")
                    (ring.mock.request/header "Accept" "text/json")
                    liberator-tutorial.core/app)
          retrieved (clojure.data.json/read-str (:body reply) :key-fn keyword)
          ]
      (is (= 200 (:status reply)))
      (is (= 4 (count retrieved)))))
  (testing "Retrieve some measures based on service, key and tags"
    (let [reply (-> (ring.mock.request/request :get "/measure?service=Test-Service-2&key=Test-Key-2&tags=%25simple%25")
                    (ring.mock.request/header "Accept" "text/json")
                    liberator-tutorial.core/app)
          retrieved (clojure.data.json/read-str (:body reply) :key-fn keyword)
          ]
      (is (= 200 (:status reply)))
      (is (= 10 (count retrieved)))))
  (testing "Retrieve some measures based on service, key and tags 2"
    (let [reply (-> (ring.mock.request/request :get "/measure?service=Test-Service-2&key=Test-Key-2&tags=%25simple%256")
                    (ring.mock.request/header "Accept" "text/json")
                    liberator-tutorial.core/app)
          retrieved (clojure.data.json/read-str (:body reply) :key-fn keyword)
          ]
      (is (= 200 (:status reply)))
      (is (= 1 (count retrieved)))))
  )


