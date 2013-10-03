(ns liberator-tutorial.core
  (:require [liberator-tutorial.db :as db]
            [liberator-tutorial.request-utils :as utils]
            [liberator-tutorial.outliers :as outliers]
            [liberator-tutorial.metric :as metric]
            [liberator-tutorial.count :as count]
            
            [liberator.core :refer [resource defresource]]
            
            [ring.adapter.jetty :refer [run-jetty]]      
            [compojure.core :refer [defroutes ANY POST GET]]
            [compojure.handler]
            [ring.middleware.params]
            [ring.middleware.keyword-params]
            [ring.util.codec :as codec]
            
            [clojure.tools.logging :refer (info error warn fatal)]
;            [clojure.data.json :as json]
;            [clj-time.core :as timecore]
;            [clj-time.coerce :as timecoerce]
;            [clojure.java.io :as io]

            ))



(defresource resource-download-metric
  :allowed-methods [:get]
  :available-media-types ["text/plain" "text/json" "text/csv" "text/xml"]
  :malformed? (fn [ctx]
                (metric/malformed-download-metric? ctx))
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     rows (db/retrieve-measures (:download-params ctx))]
                 (utils/format-result rows mediatype)
                  )))

(defresource resource-download-metric [id]
  :allowed-methods [:get]
  :available-media-types ["text/plain" "text/json" "text/csv" "text/xml"]
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     rows (db/retrieve-measures id)]
                 (utils/format-result rows mediatype)
                 ))
  )

(defresource resource-upload-metric
  :allowed-methods [:post]
  :available-media-types ["text/json"]
  :malformed? (fn [ctx]
                (metric/malformed-upload-metric? ctx))
  :post! (fn [ctx]
           (let [row-id (db/insert-measure (ctx :parsed-metric))]
             {:parsed-metric-id row-id}))
  :post-redirect? false
  :new? true
  :handle-created (fn [ctx]
                    (let [rowid (ctx :parsed-metric-id)
                          location (format "/metric/%d" rowid)]
                      (liberator.representation/ring-response {:headers {"Location" location "rowid" (str rowid)}})
                      ))
  )

(defresource count-metrics
  :allowed-methods [:get]
  :available-media-types ["text/json" "text/csv" "text/xml"]
  :malformed? (fn [ctx]
                (count/malformed-count-metrics? ctx))
  :handle-ok (fn [ctx]
               (let [selection (:download-params ctx)
                     mediatype (get-in ctx [:representation :media-type])
                     str-period (get-in ctx [:download-params :period])
                     period (utils/create-period str-period)
                     ]
                 (count/calculate-count selection mediatype period)
                 )))



(defn logg [rows]
  (info rows)
  rows)

(defresource outlier-detection
  :allowed-methods [:get]
  :available-media-types ["text/json" "text/csv" "text/xml"]
  :malformed? (fn [ctx]
                 (outliers/malformed-outliers? ctx))
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     selection (:download-params ctx)]
                 (outliers/calculate-outliers selection mediatype))))

; the data-routes are wrapped in wrap-params and wrap-result-in-json
(defroutes data-routes
  (POST "/metric" [] resource-upload-metric)
  (GET "/metric/:id" [id] (resource-download-metric id))
  (GET "/metric" [] resource-download-metric)
  (GET "/count" []  count-metrics)
  (GET "/outliers/metric" [] outlier-detection)
  )

(def app
  (-> (compojure.handler/api data-routes)))

; use (.stop server) and (.start server) in REPL
(defonce server (run-jetty #'app {:port 3000 :join? false}))
