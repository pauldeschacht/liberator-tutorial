(ns liberator-tutorial.core
  (:require [liberator.core :refer [resource defresource]]
            [ring.adapter.jetty :refer [run-jetty]]      
            [compojure.core :refer [defroutes ANY]]
            [compojure.handler]
            [ring.middleware.params]
            [ring.middleware.keyword-params]
            [ring.util.codec :as codec]

            [clojure.java.jdbc :as jdbc]
            [korma.core]
            [korma.db]
            
            [clojure.tools.logging :refer (info error warn fatal)]
            [clojure.data.json :as json]
            [clj-time.core :as timecore]
            [clj-time.coerce :as timecoerce]
            [clojure.java.io :as io]))


;;
;; database access (sqlite at the moment)
;;
(def db {:classname   "org.sqlite.JDBC"
         :subprotocol "sqlite"
         :subname     "measures.db"})

(korma.db/defdb korma-db db)

(defn create-database []
  (jdbc/with-connection db
    (jdbc/do-commands " CREATE TABLE measure ( UID INTEGER PRIMARY KEY ASC, HOST TEXT DEFAULT NULL, SERVICE TEXT NOT NULL, KEY TEXT NOT NULL, TAGS TEXT DEFAULT NULL, MEASURE FLOAT NOT NULL, TIME INTEGER NOT NULL)")))

(korma.core/defentity measure-entity
  (korma.core/database db)
  (korma.core/table :measure))

; insert a measure and return the row identifier
(defn insert-measure [measure]
  (let [korma_result (korma.core/insert measure-entity
                                        (korma.core/values [{:host (:host measure)
                                                             :service (:service measure)
                                                             :key (:key measure)
                                                             :measure (:measure measure)
                                                             :tags (:tags measure)
                                                             :time (:time measure)}]))]
    ((keyword "last_insert_rowid()") korma_result)))

(defn add-where [query condition]
  (if-not (nil? condition)
    (-> query (korma.core/where condition))
    query))

(defn retrieve-measures [params & execute?]
  (let [{:keys [host service key tags from to]} params]
    (-> (korma.core/select* measure-entity)
        (add-where (if-not (nil? host) {:host host} nil))
        (add-where {:service service})
        (add-where {:key key})
        (add-where (if-not (nil? from) {:time [korma.sql.fns/pred-> from]} nil))
        (add-where (if-not (nil? to) {:time [korma.sql.fns/pred-< to]} nil))
        (add-where (if-not (nil? tags) {:tags [korma.sql.fns/pred-like tags]} nil))
        (korma.core/exec)
        )    
    
    ))

;; convert the body to a reader. Useful for testing in the repl
;; where setting the body to a string is much simpler.
(defn body-as-string [ctx]
  (if-let [body (get-in ctx [:request :body])]
    (condp instance? body
      java.lang.String body
      (slurp (io/reader body)))))


(defn format-upload-measure [measure]
  (let [{:keys [host service key measure tags time]} measure]
    (if (or (nil? service) (nil? key) (nil? measure) (and (not (nil? time)) (not ( integer? time))))
      (let []
        (warn "format-measure failed: " measure)
        false)
      (let [time* (if (nil? time)
                    (timecoerce/to-long (timecore/now))
                    time)
            result {:host host
                    :service service
                    :key key
                    :measure measure
                    :tags tags
                    :time time*}]
        (info "format-measure result: " result)
        result))))

; check if the query contains the mandatory elements: service and key
(defn malformed-download-measure? [ctx]
  (if-let [request (:request ctx)]
    (if-let [params (:params request)]
      (let [_ (info "Params are " params)
            {:keys [host service key tags from to]} params]
        (if (or (nil? service) (nil? key))
          [true {}]
          ;first param is boolean, which is result of the function
;malformed-
          ;second param is merged with ctx, can be extracted in the
                                        ;following function
          (let [from* (if (nil? from ) 0 from)
                to* (if  (nil? to) Integer/MAX_VALUE to)]
            [false {:download-params { :host host :service service :key key :from from :to to}}]))))
    )
  )

; service, key and measure are mandatory. time is either nil or an integer
(defn malformed-upload-measure? [ctx key]
  (when (#{:put :post} (get-in ctx [:request :request-method]))
    (try
      (if-let [body (body-as-string ctx)]
        (let [_ (info "malformed-measure? body = " body)
              data (json/read-str body :key-fn keyword)
              _ (info "malformed-measure? data = " data)]
          (if-let [measure (format-upload-measure data)]
            [false {key measure}]
            {:message "Wrong format"}))
        {:message "No body"})
      (catch Exception e
        (error "malformed-measure: " e)
        {:message (format "IOException: " (.getMessage e))}))))

(defresource resource-download-measure
  :allowed-methods [:get]
  :available-media-types ["text/html" "text/json" "text/csv"]
  :malformed? (fn [ctx]
                (let [_ (info "resource-download-measure")]
                  (malformed-download-measure? ctx)))
  :handle-ok (fn [ctx]
               (let [_ (info "Download measure "(:request ctx))
                     _ (info "Extract service " (:download-params ctx))]
                  "<html>GET /download-measure is OK</html>"))
                 )

(defresource resource-upload-measure
  :allowed-methods [:post]
  :available-media-types ["text/json"]
  :malformed? (fn [ctx]
                (malformed-upload-measure? ctx :parsed-measure))
  :post! (fn [ctx]
           (let [_ (info "wellformed-measure = " (ctx :parsed-measure))
                 id (insert-measure (ctx :parsed-measure))]
             {:parsed-measure-id id}
             ))
  :post-redirect? false
  :new? true
  :handle-created (fn [ctx]
                    (let [location (format "/upload-measure/%d" (ctx :parsed-measure-id))]
                      (json/write-str {:headers {"Location" location}
                                       :body (ctx :parsed-measure)})))
  )

(defresource resource-test 
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx] (
                       let [_ (info "Test")
                            _ (info (:request ctx))]
                        "<html>Test OK</html>"
                        )
               )
  )

; the data-routes are wrapped in wrap-params and wrap-result-in-json
(defroutes data-routes
  (ANY "/upload-measure" [] resource-upload-measure)
  (ANY "/download-measure" [] resource-download-measure)
  (ANY "/test" [] resource-test)
  
  )
(defroutes my-routes
  (-> (compojure.handler/api data-routes)
      ;TODO wrap-result-in-json
      )
   ;; (ANY "/" [] (resource
   ;;             :available-media-types ["text/html"]
   ;;             :handle-ok "<html>OK</html>"))
  )

; use (.stop server) and (.start server) in REPL
(defonce server (run-jetty #'my-routes
                           {:port 3000 :join? false}))






