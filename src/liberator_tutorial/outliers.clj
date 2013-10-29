(ns liberator-tutorial.outliers
  (:require [liberator-tutorial.db-couchbase :as db]
            [liberator-tutorial.request-utils :as utils]
            [liberator-tutorial.outlier.core :as outlier-core]

            [clojure.tools.logging :refer (info error warn fatal)]
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

(defn in? [e seq]
  (some #(= e %) seq))

(defn insert-outlier-info-into-rows [rows outliers]
  (let [outliers-index (map #(:idx %) outliers)]
    (map #(if (in? %1 outliers-index)
            (assoc %2 :outlier true)
            (assoc %2 :outlier false))
         (iterate inc 0) rows))
  )

(defn calculate-outliers [selection]
  (let [rows (db/retrieve-metrics selection)
        metrics (utils/extract-metrics-from-rows rows)
        outliers (outlier-core/outliers-iqr metrics 9 1.5)]
    (insert-outlier-info-into-rows rows outliers)
    ))

(defn calculate-time-gaps [selection]
  (let [rows (db/retrieve-metrics selection)
        times (utils/extract-time-from-rows rows)
        deltas (map (fn [t1 t2] (- t2 t1)) times (rest times))
        outliers (outlier-core/outliers-iqr deltas 9 1.5)]
    (insert-outlier-info-into-rows rows outliers)

    ))
