(ns liberator-tutorial.metric
  (:require [liberator-tutorial.request-utils :as utils]

            [clojure.data.json :as json]
            
            [clj-time.core :as timecore]
            [clj-time.coerce :as timecoerce]
            [clj-time.format :as timeformat]

            [clj-message-digest.core :as digest]
            
            [clojure.tools.logging :refer (info error warn fatal)])
  
  )

(defn create-id
  "create a unique identifier for the metric. this field is used as key in couchbase."
  [metric]
  (digest/md5-hex (str (:host metric)
                       (:service metric)
                       (:key metric)
                       (:epoch metric)) ))

(defn- valid-epoch?
  "If the epoch field is present, it must be an integer. If the field is not present, the field is considered as valid."
  [epoch]
  (if (nil? epoch)
    true
    (integer? epoch))
  )

(defn- valid-iso-datetime
  "If the datetime is nil, then return default value (now). If the datetime has the format YYYY-MM-dd or the format YYYY-MM-ddThh:mm:ssZ, then return the parsed DateTime instance, otherwise return nil"
  [datetime]
  (if (nil? datetime)
    (timecore/now)
    (let [len (count datetime)]
      (if (= len 10)
        (try
          (timeformat/parse (timeformat/formatters :date) datetime)
          (catch Exception e
            nil)
          )
        (if (= len 20)
          (try
            (timeformat/parse (timeformat/formatters :date-time-no-ms) datetime)
            (catch Exception e
              nil))
          nil)
        ))))

(defn create-timestamp
  "If the metric contains a integer as epoch field, use this value as timestamp for the metric. If the epoch field is not present and the metric contains a datetime field, use the datetime as the timestamp. If neither epoch nor datetime are present, use now as timestamp for the metric.
The timestamp for a metric is available as epoch and iso-.. fields.
This function returns a map with {:epoch  :datetime }"
  [metric]
  (if (integer? (:epoch metric))
    {:epoch (:epoch metric)
     :datetime (timeformat/unparse
                (timeformat/formatters :date-time-no-ms)
                (timecoerce/from-long (:epoch metric)))}
    (if-let [datetime (valid-iso-datetime (:datetime metric))]
      {:epoch (timecoerce/to-long datetime)
       :datetime (timeformat/unparse
                  (timeformat/formatters :date-time-no-ms)
                  datetime)}
      (let [n (timecore/now)]
        {:epoch (timecoerce/to-long n)
         :datetime (timeformat/unparse
                    (timeformat/formatters :date-time-no-ms)
                    n)}))))

;;TODO: if datetime is wrong format, the default value is taken, but
;;instead a warning must be given.
(defn format-metric
  "Verify if the metric contains the mandatory fields: service, key and metric. If needed, enrich the uploaded metric with default values fields (epoch and/or datetime)."
  [metric]
  (let [{:keys [host service key metric tags epoch datetime]} metric]
    (if (or (nil? service) (nil? key) (nil? metric) (not (valid-epoch? epoch)))
      (do
        (warn "format-metric failed: " metric)
        false)
      (merge {:host host
               :service service
               :key key
               :metric metric
               :tags tags}
              (create-timestamp metric)))))

; verify if every metric respects the mandatory fields (service, key and metric)
; return a vector of formatted metrics
(defn format-upload-metrics [metrics]
  (if (map? metrics)
    (let [metrics* (format-metric metrics)]
      (if (false? metrics*)
        false
        [metrics*])
      )
    (let [metrics* (map #(format-upload-metric %1) metrics)]
      (if (some false? metrics*)
        false
        metrics*))))

; check if the query contains the mandatory elements: service and key
(defn malformed-download-metric? [ctx]
  (if-let [request (:request ctx)]
    (if-let [params (:params request)]
      (let [{:keys [host service key tags from to]} params
            _ (info "download metric params: " params)]
        (if (or (nil? service) (nil? key))
          [true {}]
          ;first param is boolean, which is result of the function malformed?
          ;second param is merged with ctx, can be extracted in the following function (handler-ok or post!)
          (let [from* (if (nil? from ) 0 from)
                to* (if  (nil? to) Integer/MAX_VALUE to)]
            [false {:download-params {:host host :service service :key key :from from :to to :tags tags}}]))))
    )
  )

; service, key and metric are mandatory. time is either nil or an integer
(defn malformed-upload-metric? [ctx]
  (when (#{:put :post} (get-in ctx [:request :request-method]))
    (try
      (if-let [body (utils/body-as-string ctx)]
        (let [
              data (json/read-str body :key-fn keyword)
              
              ]
          (if-let [metrics (format-upload-metrics data)]
            [false {:parsed-metric metrics}]
            [true {:message "Wrong format"}]))
        [true {:message "No body"}])
      (catch Exception e
        (error "malformed-upload-metric: " e)
        {:message (format "IOException: " (.getMessage e))}))))

