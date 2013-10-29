(ns liberator-tutorial.core
  (:require [liberator-tutorial.db-couchbase :as db]
            [liberator-tutorial.request-utils :as utils]
            [liberator-tutorial.outliers :as outliers]
            [liberator-tutorial.metric :as metric]
            [liberator-tutorial.count :as count]
            [liberator-tutorial.ws-channels :as ws]
            
            [liberator.core :refer [resource defresource]]
            [liberator.conneg]
            [clojure.java.io :as io]

                                        ;            [ring.adapter.jetty :refer [run-jetty]]
            [org.httpkit.server :as httpkit]
            [compojure.core :refer [defroutes ANY POST GET PUT DELETE]]
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
                (metric/malformed-download-metric? ctx))
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     rows (db/retrieve-metrics (:download-params ctx))]
                 (utils/format-result rows mediatype)
                 )))

(defn insert-rows-in-response [rows]
  (liberator.representation/ring-response {:body rows}))

(defresource resource-delete-metric
  :allowed-methods [:delete]
  :available-media-types ["text/html" "text/json" "text/csv" "text/xml"]
  :malformed? (fn [ctx]
                (metric/malformed-download-metric? ctx))
  :delete! (fn [ctx]
             (let [mediatype (get-in ctx [:representation :media-type])]
               (->
                (db/delete-metrics (:download-params ctx))
                (error)
                (utils/format-result mediatype)
                (insert-rows-in-response))))
  :delete-enacted? true
  )



(defresource resource-download-metric-id [id]
  :allowed-methods [:get]
  :available-media-types ["text/json" "text/csv" "text/xml" "text/plain"]
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     row (db/retrieve-metric id)]
                 (utils/format-result row mediatype)
                 )))

(defresource resource-upload-metric
  :allowed-methods [:post]
  :available-media-types ["text/json"]
  :malformed? (fn [ctx]
                (metric/malformed-upload-metric? ctx)
                )
  :post! (fn [ctx]
           (let [
                 inserted-rows (db/insert-metrics (ctx :parsed-metric))
                 ]
;             (ws/update-ws-clients (ctx :query-params) (ctx :parsed-metric))
           {:inserted-metrics inserted-rows})
           )
  :post-redirect? false
  :new? true
  :handle-created (fn [ctx]
                    (let [inserted-metrics (ctx :inserted-metrics)
                          _ (info inserted-metrics)]
                      (->>
                       (map #(assoc % :href (str "/metric/" (:id %))) inserted-metrics)
                       (map #(dissoc % :id))
                       (insert-rows-in-response))
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
                     selection (:download-params ctx)]
                 (-> (outliers/calculate-outliers selection)
                     (utils/format-result mediatype)))))

(defresource time-gap-detection
  :allowed-methods [:get]
  :available-media-types ["text/csv" "text/json" "text/xml"]
  :malformed? (fn [ctx]
                (metric/malformed-download-metric? ctx))
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     selection (:download-params ctx)]
                 (-> (outliers/calculate-time-gaps selection)
                     (utils/format-result mediatype)))))


(defn web-socket [request]
  (httpkit/with-channel request channel
    (println request)
    (ws/add-channel request channel)
    (httpkit/on-close channel (fn [status]
                        (ws/remove-channel channel)))
    (httpkit/on-receive channel (fn [data]
                          (httpkit/send! channel data)))
    ))


; the data-routes are wrapped in wrap-params and wrap-result-in-json
(defroutes data-routes
  (POST "/metric" [] resource-upload-metric)
  (GET "/metric/:id" [id] (resource-download-metric-id id))
  (GET "/metric" [] resource-download-metric)
  (DELETE "/metric" [] resource-delete-metric)
  (GET "/count" []  count-metrics)
  (GET "/outliers/metric" [] outlier-detection)
  (GET "/timegap/metric" [] time-gap-detection)
  (GET "/static/:resource" [resource] static-resource)
  (GET "/ws/*" [] web-socket)
  )

(def app
  (-> (compojure.handler/api data-routes)))

; with jetty use (.stop server) and (.start server) in REPL
;(defonce server (run-jetty #'app {:port 3000 :join? false}))

; httpkit the run-server returns a function that stops the server
; call (server) to stop the service
(def server (httpkit/run-server #'app {:port 3000 :join? false}))
