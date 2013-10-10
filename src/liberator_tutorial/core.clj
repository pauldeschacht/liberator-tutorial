(ns liberator-tutorial.core
  (:require [liberator-tutorial.db :as db]
            [liberator-tutorial.request-utils :as utils]
            [liberator-tutorial.outliers :as outliers]
            [liberator-tutorial.metric :as metric]
            [liberator-tutorial.count :as count]
            
            [liberator.core :refer [resource defresource]]
            [liberator.conneg]
            [clojure.java.io :as io]

            [ring.adapter.jetty :refer [run-jetty]]      
            [compojure.core :refer [defroutes ANY POST GET]]
            [compojure.handler]
            [ring.middleware.params]
            [ring.middleware.keyword-params]
            [ring.util.codec :as codec]
            [ring.util.mime-type :only [ext-mime-type]]
            
            [clojure.tools.logging :refer (info error warn fatal)]
            ))

(defn logg [rows]
  (info rows)
  rows)


(defresource static-resource [resource]
  :available-media-types #(let [file (get-in % [:request :route-params :resource])]      
                            (if-let [mime-type (ring.util.mime-type/ext-mime-type file)]
                              [mime-type]
                              []))
 
  :exists? #(let [file (get-in % [:request :route-params :resource])]      
              (let [f (io/file "public/static/" file)]
                [(.exists f) {::file f}]))
     
  :handle-ok (fn [{{{file :resource} :route-params} :request}]                
               (io/file "public/static/" file))

  :last-modified (fn [{{{file :resource} :route-params} :request}]                                                              
                   (.lastModified (io/file "public/static/" file))))

(defresource resource-download-metric
  :allowed-methods [:get]
  :available-media-types ["text/plain" "text/json" "text/csv" "text/xml"]
  :malformed? (fn [ctx]
                (logg ctx)
                (logg (get-in ctx [:request :headers "accept"]))
                (logg (get-in ctx [:resource :available-media-types]))
                (logg "----------------")
                (logg (liberator.conneg/best-allowed-content-type (get-in ctx [:request :headers "accept"])
                                                                  ["text/plain" "text/csv" "text/json"]))
                (metric/malformed-download-metric? ctx))
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     _ (logg mediatype)
                     rows (db/retrieve-measures (:download-params ctx))
]
                 
                 (utils/format-result rows mediatype)
                  )))

(defresource resource-download-metric [id]
  :allowed-methods [:get]
  :available-media-types ["text/plain" "text/json" "text/csv" "text/xml"]
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     rows (db/retrieve-measures id)]
                 (utils/format-result rows mediatype)
                 )))

(defresource resource-upload-metric
  :allowed-methods [:post]
  :available-media-types ["text/json"]
  :malformed? (fn [ctx]
                (metric/malformed-upload-metric? ctx))
  :post! (fn [ctx]
           (let [row-id (db/insert-measures (ctx :parsed-metric))]
             {:parsed-metric-id row-id}))
  :post-redirect? false
  :new? true
  :handle-created (fn [ctx]
                    (let [rowid (ctx :parsed-metric-id)
                          location (format "/metric/%d" rowid)]
                      (liberator.representation/ring-response {:headers {"Location" location "rowid" (str rowid)}})
                      )))

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
                 (-> 
                  (count/calculate-count selection mediatype period)
                  (utils/format-result mediatype))
                 )))


(defresource outlier-detection
  :allowed-methods [:get]
  :available-media-types ["text/csv" "text/json" "text/xml"]
  :malformed? (fn [ctx]
                (logg ctx)
                 (outliers/malformed-outliers? ctx))
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     selection (:download-params ctx)
                     _ (info "Selection mediatype " mediatype)
                     _ (info "Representation " (:representation ctx))]
                 (-> (outliers/calculate-outliers selection)
                     (utils/format-result mediatype)))))

; the data-routes are wrapped in wrap-params and wrap-result-in-json
(defroutes data-routes
  (POST "/metric" [] resource-upload-metric)
  (GET "/metric/:id" [id] (resource-download-metric id))
  (GET "/metric" [] resource-download-metric)
  (GET "/count" []  count-metrics)
  (GET "/outliers/metric" [] outlier-detection)
  (GET "/static/:resource" [resource] static-resource)
  )

(def app
  (-> (compojure.handler/api data-routes)))

; use (.stop server) and (.start server) in REPL
(defonce server (run-jetty #'app {:port 3000 :join? false}))
