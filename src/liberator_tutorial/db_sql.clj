(ns liberator-tutorial.db-sql
  (:require [clojure.java.jdbc :as jdbc]
            [korma.core]
            [korma.db]
            
            [clojure.tools.logging :refer (info error warn fatal)]
            [clojure.data.json :as json]))
 
;;
;; database access (sqlite at the moment)
;;
(def db {:classname   "org.sqlite.JDBC"
         :subprotocol "sqlite"
         :subname     "metrics.db"})

(korma.db/defdb korma-db db)

(korma.core/defentity metric-entity
  (korma.core/database db)
  (korma.core/table :metric))

;TODO use korma
(defn create-metric-table [db]
  (jdbc/with-connection db
    (jdbc/do-commands "CREATE TABLE metric ( uid INTEGER PRIMARY KEY ASC, host TEXT DEFAULT NULL, service TEXT NOT NULL, key TEXT NOT NULL, tags TEXT DEFAULT NULL, metric FLOAT NOT NULL, time INTEGER NOT NULL)")))

;TODO use korma
(defn drop-metric-table [db]
  (jdbc/with-connection db
    (jdbc/do-commands "DROP TABLE IF EXISTS metric;")))


; helper function to add a where clause
(defn add-where [query condition]
  (if-not (nil? condition)
    (-> query (korma.core/where condition))
    query))

; returns the query as sql string
(defn retrieve-metrics-debug [params & execute?]
  (let [{:keys [host service key tags from to]} params
        _ (warn params)]
    (-> (korma.core/select* metric-entity)
        (add-where (if-not (nil? host) {:host host} nil))
        (add-where {:service service})
        (add-where {:key key})
        (add-where (if-not (nil? from) {:time [korma.sql.fns/pred->= from]} nil))
        (add-where (if-not (nil? to) {:time [korma.sql.fns/pred-<= to]} nil))
        (add-where (if-not (nil? tags) {:tags [korma.sql.fns/pred-like tags]} nil))
        (korma.core/as-sql)
        )    
    ))

; insert a metric and return the row identifier
(defn insert-metric [metric]
  (let [korma_result (korma.core/insert metric-entity
                                        (korma.core/values
                                         [{:host (:host metric)
                                           :service (:service metric)
                                           :key (:key metric)
                                           :metric (:metric metric)
                                           :tags (:tags metric)
                                           :time (:time metric)}]))
        ]
    ((keyword "last_insert_rowid()") korma_result)))

(defn insert-metrics [metrics]
  ;; following expression does not work (fails in sqlite)
  ;; (let [korma_result (korma.core/insert metric-entity (korma.core/values (vec metrics)))
  ;;       _ (info "sql = " korma_result)]
  ;;   ((keyword "last_insert_rowid()") korma_result))
  
  (let [result (map #(insert-metric %1) metrics)]
    (first result))
)


; retrieve the metrics: service and key are mandatory
(defn retrieve-metrics [params & execute?]
  (let [{:keys [host service key tags from to]} params
        _ (info params)
        ]
    (-> (korma.core/select* metric-entity)
        (add-where (if-not (nil? host) {:host host} nil))
        (add-where {:service service})
        (add-where {:key key})
        (add-where (if-not (nil? from) {:time [korma.sql.fns/pred->= from]} nil))
        (add-where (if-not (nil? to) {:time [korma.sql.fns/pred-<= to]} nil))
        (add-where (if-not (nil? tags) {:tags [korma.sql.fns/pred-like tags]} nil))
        (korma.core/exec)     
        )
    ))

(defn retrieve-metric [id]
  (-> (korma.core/select* metric-entity)
      (add-where {:rowid id})
      (korma.core/exec)))

