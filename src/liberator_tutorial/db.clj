(ns liberator-tutorial.db
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
         :subname     "measures.db"})

(korma.db/defdb korma-db db)

(korma.core/defentity measure-entity
  (korma.core/database db)
  (korma.core/table :measure))

;TODO use korma
(defn create-measure-table [db]
  (jdbc/with-connection db
    (jdbc/do-commands "CREATE TABLE measure ( uid INTEGER PRIMARY KEY ASC, host TEXT DEFAULT NULL, service TEXT NOT NULL, key TEXT NOT NULL, tags TEXT DEFAULT NULL, measure FLOAT NOT NULL, time INTEGER NOT NULL)")))

;TODO use korma
(defn drop-measure-table [db]
  (jdbc/with-connection db
    (jdbc/do-commands "DROP TABLE IF EXISTS measure;")))

; insert a measure and return the row identifier
(defn insert-measure [measure]
  (let [korma_result (korma.core/insert measure-entity
                                        (korma.core/values
                                         [{:host (:host measure)
                                           :service (:service measure)
                                           :key (:key measure)
                                           :measure (:metric measure)
                                           :tags (:tags measure)
                                           :time (:time measure)}]))
        ]
    ((keyword "last_insert_rowid()") korma_result)))

(defn insert-measures [measures]
  ;; following expression does not work (fails in sqlite)
  ;; (let [korma_result (korma.core/insert measure-entity (korma.core/values (vec measures)))
  ;;       _ (info "sql = " korma_result)]
  ;;   ((keyword "last_insert_rowid()") korma_result))
  
  (let [result (map #(insert-measure %1) measures)]
    (first result))
)

; helper function to add a where clause
(defn add-where [query condition]
  (if-not (nil? condition)
    (-> query (korma.core/where condition))
    query))

; returns the query as sql string
(defn retrieve-measures-debug [params & execute?]
  (let [{:keys [host service key tags from to]} params
        _ (warn params)]
    (-> (korma.core/select* measure-entity)
        (add-where (if-not (nil? host) {:host host} nil))
        (add-where {:service service})
        (add-where {:key key})
        (add-where (if-not (nil? from) {:time [korma.sql.fns/pred->= from]} nil))
        (add-where (if-not (nil? to) {:time [korma.sql.fns/pred-<= to]} nil))
        (add-where (if-not (nil? tags) {:tags [korma.sql.fns/pred-like tags]} nil))
        (korma.core/as-sql)
        )    
    ))

; retrieve the measures: service and key are mandatory
(defn retrieve-measures [params & execute?]
  (let [{:keys [host service key tags from to]} params
        _ (info params)
        ]
    (-> (korma.core/select* measure-entity)
        (add-where (if-not (nil? host) {:host host} nil))
        (add-where {:service service})
        (add-where {:key key})
        (add-where (if-not (nil? from) {:time [korma.sql.fns/pred->= from]} nil))
        (add-where (if-not (nil? to) {:time [korma.sql.fns/pred-<= to]} nil))
        (add-where (if-not (nil? tags) {:tags [korma.sql.fns/pred-like tags]} nil))
        (korma.core/exec)     
        )
    ))

(defn retrieve-measure [id]
  (-> (korma.core/select* measure-entity)
      (add-where {:rowid id})
      (korma.core/exec)))

