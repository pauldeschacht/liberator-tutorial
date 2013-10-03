(ns liberator-tutorial.outliers
  (:require [liberator-tutorial.db :as db]
            [liberator-tutorial.request-utils :as utils]
            [liberator-tutorial.outlier.core :as outlier-core]
            ))

(defn malformed-outliers? [ctx]
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

(defn calculate-outliers [selection mediatype]
  (-> (db/retrieve-measures selection)
      (utils/extract-metrics-from-rows)
      (outlier-core/outliers-iqr 9 1.5)
      (utils/format-result mediatype)))
