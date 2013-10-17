(ns liberator-tutorial.core
  (:require [liberator-tutorial.db :as db]
            [liberator-tutorial.request-utils :as utils]
            [liberator-tutorial.outliers :as outliers]
            [liberator-tutorial.metric :as metric]
            [liberator-tutorial.count :as count]
            
            [liberator.core :refer [resource defresource]]
            [liberator.conneg]
            [clojure.java.io :as io]

                                        ;            [ring.adapter.jetty :refer [run-jetty]]
            [org.httpkit.server :as httpkit]
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

(def ws-channels (atom {}))

(defn add-channel
  "add a channel to the list of subscriptions. The subscription is based on a key."
  [key channel]
  (info "adding channel with key " key)
  (swap! ws-channels #(assoc % key (if ( nil? (get % key))
                                      [channel]
                                      (conj (get % key) channel)))))

(defn remove-channel
  "remove the channel from *all* the subscriptions/keys"
  [remove-me-channel]
  (info "removing channel")
  (swap! ws-channels (fn [keyed-channels]
                       (apply merge {}
                              (map (fn [key channels]
                                     {key (filter
                                           #(not = % remove-me-channel)
                                           channels)})
                                   (keys keyed-channels)
                                   (vals keyed-channels))))))

(defn get-channels
  "first version: on take into account the exact same key
TODO: make flexible: subscriber to {service MIDT} must also receive updates for {host some_machine service MIDT}"
  
  [key]
  (get @ws-channels key)
  )

(defn update-ws-clients [request data]
  (let [info {:status 200
              :headers {"Content-Type" "application/json; charset=utf-8"}
              :body (str "Hello from metric upload: " data)}] ;;data must be a
    ;;string 
    (doseq [channel (get-channels request)]
      (httpkit/send! channel info))))




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
             (update-ws-clients
              (ctx :query-params)
              (ctx :parsed-metric))
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


(defn web-socket [request]
  (httpkit/with-channel request channel
    (println request)
    (add-channel request channel)
    (httpkit/on-close channel (fn [status]
                        (info "channel closed")
                        (remove-channel channel)))
    (httpkit/on-receive channel (fn [data]
                          (info "received data from channel " data)
                          (httpkit/send! channel data)))
    ))


; the data-routes are wrapped in wrap-params and wrap-result-in-json
(defroutes data-routes
  (POST "/metric" [] resource-upload-metric)
  (GET "/metric/:id" [id] (resource-download-metric id))
  (GET "/metric" [] resource-download-metric)
  (GET "/count" []  count-metrics)
  (GET "/outliers/metric" [] outlier-detection)
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
