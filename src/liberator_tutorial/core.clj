(ns liberator-tutorial.core
  (:require [liberator.core :refer [resource defresource]]
            [ring.adapter.jetty :refer [run-jetty]]      
            [compojure.core :refer [defroutes ANY]]
            [ring.middleware.params]
            [ring.middleware.keyword-params]
            [ring.util.codec :as codec]
            [clojure.tools.logging :refer (info error warn fatal)]
            [clojure.data.json :as json]
            [clj-time.core :as timecore]
            [clj-time.coerce :as timecoerce]
            [clojure.java.io :as io]))

;; local implementation of ring.middleware.wrap-params
;; I don't know why the handler is not doing it for me
(defn- keyword-syntax? [s]
  (re-matches #"[A-Za-z*+!_?-][A-Za-z0-9*+!_?-]*" s))

(defn- keyify-params [target]
  (cond
    (map? target)
      (into {}
        (for [[k v] target]
          [(if (and (string? k) (keyword-syntax? k))
             (keyword k)
             k)
           (keyify-params v)]))
    (vector? target)
      (vec (map keyify-params target))
    :else
      target))

(defn- parse-params [params encoding]
  (let [params (codec/form-decode params encoding)]
    (if (map? params) params {})))

(defn- assoc-query-params
  "Parse and assoc parameters from the query string with the request."
  [request encoding]
  (merge-with merge request
    (if-let [query-string (:query-string request)]
      (let [params (parse-params query-string encoding)]
        {:query-params params, :params params})
      {:query-params {}, :params {}})))

(defn- extract-params
  [request]
  (let [encoding (or (:character-encoding request)
                     "UTF-8")
        request* (assoc-query-params request encoding)
        params (keyify-params (:params request*))
        ]
    params))

;; convert the body to a reader. Useful for testing in the repl
;; where setting the body to a string is much simpler.
(defn body-as-string [ctx]
  (if-let [body (get-in ctx [:request :body])]
    (condp instance? body
      java.lang.String body
      (slurp (io/reader body)))))

(defn insert-measure [measure]
  true)

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
    (let [_ (info "Query is " request)
          params (extract-params request)
          _ (info "Params are " params)
          {:keys [host service key tags time]} params
          _ (info "Service = " service " Key = " key )]
      (if (or (nil? service) (nil? key))
        [true {}]
        [false {:query-params { :host host :service service :key key}}]))
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
            [false {key data}]
            {:message "Wrong format"}))
        {:message "No body"})
      (catch Exception e
        (error "malformed-measure: " e)
        {:message (format "IOException: " (.getMessage e))}))))

(defroutes routes
  (ANY "/upload-measure" []
       (resource :allowed-methods [:post]
                 :available-media-types ["text/json"]
                 :malformed? #(malformed-upload-measure? % :parsed-measure)
                 :post! (fn [ctx]
                          (let [_ (info "wellformed-measure = " (ctx :parsed-measure))
                                id (insert-measure (ctx :parsed-measure))]
                            {:parsed-measure-id 45}
                            ))
                 :post-redirect? false
                 :new? true
                 :handle-created (fn [ctx]
                                   (let [location (format "/upload-measure/%d" (ctx :parsed-measure-id))]
                                     (json/write-str {:headers {"Location" location}
                                                      :body (ctx :parsed-measure)})))
                 )
       )

  (ANY "/download-measure" []
       (resource :allowed-methods [:get]
;                 :available-media-types ["text/json" "text/csv"]
                 :malformed? (fn [ctx]
                                        ;                  (malformed-download-measure? ctx)
                               false
                               )
                 :handler-ok (fn [ctx]
                               "GET is OK")
                 )
       
       )
  (ANY "/" []
       (resource :available-media-types ["text/html"]
                 :handle-ok "<html>OK</html>")))

(def handler
  (-> routes
      ring.middleware.params/wrap-params
      ring.middleware.keyword-params/wrap-keyword-params))

; use (.stop server) and (.start server) in REPL
(defonce server (run-jetty #'handler {:port 3000 :join? false}))






