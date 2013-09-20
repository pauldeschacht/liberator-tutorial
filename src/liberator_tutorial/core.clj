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

; verify if every measure respects the mandatory fields (service, key and measure)
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
      (let [{:keys [host service key tags from to]} params]
        (if (or (nil? service) (nil? key))
          [true {}]
          ;first param is boolean, which is result of the function malformed?
          ;second param is merged with ctx, can be extracted in the following function (handler-ok or post!)
          (let [from* (if (nil? from ) 0 from)
                to* (if  (nil? to) Integer/MAX_VALUE to)]
            [false {:download-params {:host host :service service :key key :from from :to to :tags tags}}]))))
    )
  )

(defn malformed-count-measures? [ctx]
  (if-let [request (:request ctx)]
    (if-let [params (:params request)]
      (let [{:keys [host service key tags from to period]} params]
        (if (or (nil? service) (nil? key))
          [true {}]
          ;first param is boolean, which is result of the function malformed?
          ;second param is merged with ctx, can be extracted in the following function (handler-ok or post!)
          [false {:download-params {:host host :service service :key key :from from :to to :tags tags :period period}}])))
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
  (json/write-str rows :key-fn #(clojure.string/lower-case (name  %))))

(defn rows-to-csv [rows]
  (apply str (interpose "\n" (map #(row-to-csv %1) rows))))

(defn format-result [rows mediatype]
  (condp = mediatype
    "text/csv" (rows-to-csv rows)
    "text/json" (rows-to-json rows)
    "text/xml" (rows-to-xml rows)
    (rows-to-csv rows))
  )

(defresource resource-download-measures
  :allowed-methods [:get]
  :available-media-types ["text/plain" "text/json" "text/csv" "text/xml"]
  :malformed? (fn [ctx]
                (malformed-download-measure? ctx))
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     rows (db/retrieve-measures (:download-params ctx))]
                 (format-result rows mediatype)
                  )))

(defresource resource-download-measure [id]
  :allowed-methods [:get]
  :available-media-types ["text/plain" "text/json" "text/csv" "text/xml"]
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     rows (db/retrieve-measure id)]
                 (format-result rows mediatype)
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
                      (liberator.representation/ring-response {:headers {"Location" location "rowid" (str rowid)}})
                      ))
  )


; extract from the row only the key-value pairs specified in period
; (def row {:year 2013 :month 05 :day-in-month 18 :measure ... })
; (def period [:year :month])
; results in {:year 2013 :month 05}
(defn create-group-key-by-period [row period]
  (apply merge (map #(hash-map % (get row %)) period)))

; group the data by period (period is a vector of keys)
; (def period [:year :month]
(defn group-data-by-period [data period]
  (group-by #(create-group-key-by-period % period) data)
  )

; once the data is grouped per period (that is a list of measures
; associated with a period), apply a aggregate function on
; that list of measures
; input: [ {period1} [ list of measures ] {period2} [list of measures]
; ]
; output: [ {period1} aggregate1 {period2} aggregate2 ]

(defn aggregate-data-by-period [data aggregate-fn]
  (map #(hash-map (key %) (aggregate-fn (val %))) data))

(defn count-data-by-period [data]
  (aggregate-data-by-period data count))

(defn flatten-grouped-data [data key-name]
  (map #(assoc (first (keys %)) key-name (first (vals %))) data  ))

(defn create-period [str-period]
  (condp = str-period
    "monthly" [:year :month]
    "weekly"  [:year :week]
    "daily" [:year :month :day-in-month]
    [:year :month]))

(defn expand-epoch-row [row]
  (let [dt (timecoerce/from-long (* 1000 (:TIME row)))
        year (timecore/year dt)
        month (timecore/month dt)
        dd (timecore/day dt)
        week (.getWeekyear dt)]
    {:year year :month month :day-in-month dd :week week}))

(defn expand-epoch [rows]
  (map #(merge % (expand-epoch-row %)) rows))

(defresource count-measures
  :allowed-methods [:get]
  :available-media-types ["text/json" "text/csv" "text/xml"]
  :malformed? (fn [ctx]
                (malformed-count-measures? ctx))
  :handle-ok (fn [ctx]
               (let [mediatype (get-in ctx [:representation :media-type])
                     str-period (get-in ctx [:download-params :period])
                     period (create-period str-period)
                     result (-> (db/retrieve-measures (:download-params ctx))
                                (expand-epoch)
                                (group-data-by-period period)
                                (count-data-by-period)
                                (flatten-grouped-data :number-files)
                                (format-result mediatype))
                     ]
                 result))
  )

; the data-routes are wrapped in wrap-params and wrap-result-in-json
(defroutes data-routes
  (POST "/measure" [] resource-upload-measure)
  (GET "/measure/:id" [id] (resource-download-measure id))
  (GET "/measure" [] resource-download-measures)
  (GET "/count" []  count-measures)
  )

(def app
  (-> (compojure.handler/api data-routes)))

; use (.stop server) and (.start server) in REPL
(defonce server (run-jetty #'app {:port 3000 :join? false}))






