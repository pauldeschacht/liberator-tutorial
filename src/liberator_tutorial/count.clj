(ns liberator-tutorial.count
  (:require [liberator-tutorial.db :as db]
            [liberator-tutorial.request-utils :as utils])
)
(defn malformed-count-metrics? [ctx]
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

(defn count-data-by-period [data]
  (utils/aggregate-data-by-period data count))

(defn calculate-count [selection mediatype period]
  (-> (db/retrieve-measures selection)
      (utils/epoch-to-date-rows)
      (utils/group-data-by-period period)
      (count-data-by-period)
      (utils/flatten-grouped-data :number-files)
      (utils/format-result mediatype))
  
  )

