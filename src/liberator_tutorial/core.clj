(ns liberator-tutorial.core
  (:require [liberator-tutorial.db :as db]
            [liberator.core :refer [resource defresource]]
   
            [ring.adapter.jetty :refer [run-jetty]]      
            [compojure.core :refer [defroutes ANY POST GET]]
            [compojure.handler]
            [ring.middleware.params]
            [ring.middleware.keyword-params]
            [ring.util.codec :as codec]
            
            [clojure.tools.logging :refer (info error warn fatal)]
            [clojure.data.json :as json]
            [clj-time.core :as timecore]
            [clj-time.coerce :as timecoerce]
            [clojure.java.io :as io]))

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
        result))))

; verify if every measure respects the mandatory fields (service, key
                                        ; and measure)
; return a vector of formatted measures
(defn format-upload-measures [measures]
  (if (map? measures)
    (let [measures* (format-upload-measure measures)]
      (if (false? measures*)
        false
        [measures*])
      )
    (let [measures* (map #(format-upload-measure %1) measures)]
      (if (some false? measures*)
        false
        measures*))))

; check if the query contains the mandatory elements: service and key
(defn malformed-download-measure? [ctx]
  (if-let [request (:request ctx)]
    (if-let [params (:params request)]
      (let [_ (info "Params are " params)
            {:keys [host service key tags from to]} params]
        (if (or (nil? service) (nil? key))
          [true {}]
          ;first param is boolean, which is result of the function malformed?
          ;second param is merged with ctx, can be extracted in the following function (handler-ok or post!)
          (let [from* (if (nil? from ) 0 from)
                to* (if  (nil? to) Integer/MAX_VALUE to)]
            [false {:download-params { :host host :service service :key key :from from :to to}}]))))
    )
  )

; service, key and measure are mandatory. time is either nil or an integer
(defn malformed-upload-measure? [ctx]
  (when (#{:put :post} (get-in ctx [:request :request-method]))
    (try
      (if-let [body (body-as-string ctx)]
        (let [data (json/read-str body :key-fn keyword)]
          (if-let [measures (format-upload-measures data)]
            [false {:parsed-measure measures}]
            [true {:message "Wrong format"}]))
        [true {:message "No body"}])
      (catch Exception e
        (error "malformed-upload-measure: " e)
        {:message (format "IOException: " (.getMessage e))}))))

(defn row-to-csv [row]
  (let [{:keys [HOST SERVICE KEY TIME MEASURE TAGS]} row]
    (apply str (interpose "," [HOST SERVICE KEY TIME MEASURE TAGS]))))

(defn row-to-hiccup [row]
  (let [{:keys [HOST SERVICE KEY TIME MEASURE TAGS]} row]
    [:measure [:host HOST] [:service SERVICE] [:key KEY] [:time TIME] [:measure MEASURE] [:tags TAGS]]))

(defn rows-to-xml [rows]
  (hiccup.core/html
   [:measures (map #(row-to-hiccup %1) rows)]))

(defn rows-to-json [rows]
  (json/write-str rows :key-fn #(clojure.string/lower-case %)))

(defn rows-to-csv [rows]
  (apply str (interpose "\n" (map #(row-to-csv %1) rows))))

(defresource resource-download-measures
  :allowed-methods [:get]
  :available-media-types ["text/plain" "text/json" "text/csv" "text/xml"]
  :malformed? (fn [ctx]
                (malformed-download-measure? ctx))
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     result (db/retrieve-measures (:download-params ctx))]
                 (condp = mediatype
                   "text/csv" (rows-to-csv result)
                   "text/json" (rows-to-json result)
                   "text/xml" (rows-to-xml result)
                   (rows-to-csv result))
                  )))

(defresource resource-download-measure [id]
  :allowed-methods [:get]
  :available-media-types ["text/plain" "text/json" "text/csv" "text/xml"]
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     result (db/retrieve-measure id)]
                 (condp = mediatype
                   "text/csv" (rows-to-csv result)
                   "text/json" (rows-to-json result)
                   "text/xml" (rows-to-xml result)
                   (rows-to-csv result))
                 ))
  )

(defresource resource-upload-measure
  :allowed-methods [:post]
  :available-media-types ["text/json"]
  :malformed? (fn [ctx]
                (malformed-upload-measure? ctx))
  :post! (fn [ctx]
           (let [row-id (db/insert-measures (ctx :parsed-measure))]
             {:parsed-measure-id row-id}))
  :post-redirect? false
  :new? true
  :handle-created (fn [ctx]
                    (let [rowid (ctx :parsed-measure-id)
                          location (format "/upload-measure/%d" rowid)]
                      (json/write-str {:headers {"Location" location}
                                       :rowid rowid
                                       ;:body (ctx :parsed-measure)
                                       })))
  )

; the data-routes are wrapped in wrap-params and wrap-result-in-json
(defroutes data-routes
  (POST "/measure" [] resource-upload-measure)
  (GET "/measure/:id" [id] (resource-download-measure id))
  (GET "/measure" [] resource-download-measures))

(def app
  (-> (compojure.handler/api data-routes)))

; use (.stop server) and (.start server) in REPL
(defonce server (run-jetty #'app {:port 3000 :join? false}))






