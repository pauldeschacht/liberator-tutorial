(ns liberator-tutorial.metric
  (:require [liberator-tutorial.request-utils :as utils]

            [clojure.data.json :as json]
            
            [clj-time.core :as timecore]
            [clj-time.coerce :as timecoerce]
            
            [clojure.tools.logging :refer (info error warn fatal)])
  
  )


(defn format-upload-metric [metric]
  (let [{:keys [host service key metric tags time]} metric]
    (if (or (nil? service) (nil? key) (nil? metric) (and (not (nil? time)) (not ( integer? time))))
      (let []
        (warn "format-metric failed: " metric)
        false)
      (let [time* (if (nil? time)
                    (timecoerce/to-long (timecore/now))
                    time)
            result {:host host
                    :service service
                    :key key
                    :metric metric
                    :tags tags
                    :time time*}]
        result))))

; verify if every metric respects the mandatory fields (service, key and metric)
; return a vector of formatted metrics
(defn format-upload-metrics [metrics]
  (if (map? metrics)
    (let [metrics* (format-upload-metric metrics)]
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

; service, key and metric are mandatory. time is either nil or an integer
(defn malformed-upload-metric? [ctx]
  (when (#{:put :post} (get-in ctx [:request :request-method]))
    (try
      (if-let [body (utils/body-as-string ctx)]
        (let [data (json/read-str body :key-fn keyword)]
          (if-let [metrics (format-upload-metrics data)]
            [false {:parsed-metric metrics}]
            [true {:message "Wrong format"}]))
        [true {:message "No body"}])
      (catch Exception e
        (error "malformed-upload-metric: " e)
        {:message (format "IOException: " (.getMessage e))}))))

