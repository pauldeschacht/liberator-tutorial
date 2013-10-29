(ns liberator-tutorial.db-couchbase
  (:require [liberator-tutorial.metric :as metric]
            
            [couchbase-clj.client :as c]
            [couchbase-clj.query :as q]
            [clojure.data.json :as json]

            [clojure.tools.logging :refer (info error warn fatal)]))

(c/defclient client {:bucket "default"
                     :password ""
                     :uris ["http://127.0.0.1:8091/pools"]
                     })

(def service-key-view (c/get-view client "dev_metric" "servicekey"))
(def host-service-key-view (c/get-view client "dev_metric" "hostservicekey"))

(defn- timestamp [t]
  (if (nil? t)
    ""
    (str " " t)))

(defn- merge-docs-with-ids
  "merge the document id with the document"
  [rows]
  (println "merge-docs-with-ids")
  (map (fn [a b] (merge a b))
       (map c/view-doc-json rows)
       (map #(hash-map :id %) (map c/view-id rows))))

(defn- retrieve-metrics-service-key [service key from to]
  (let [_ (info "retrieve service key " service "," key "," from "," to)
        startkey (if (nil? from)
                   {:range-start (vector service key)}
                   {:range-start (vector service key from)})
        endkey (if (nil? to)
                 {}
                 {:range-end (vector service key to)})
        query (q/create-query
               (merge {:include-docs true} startkey endkey))

        rows (c/query client service-key-view query)
        ]
    (merge-docs-with-ids rows)
    )
  )
(defn- retrieve-metrics-host-service-key [host service key from to]
  (let [
        startkey (if (nil? from)
                   {:range-start (vector host service key)}
                   {:range-start (vector host service key from)})
        endkey (if (nil? to)
                 {}
                 {:range-end (vector host service key to)})
        query (q/create-query
               (merge {:include-docs true} startkey endkey))

        rows (c/query client service-key-view query)
        ]
    (merge-docs-with-ids rows)
    )
  )

(defn insert-metric [metric]
  (let [id (metric/create-id metric)
        _ (info "hash is " id)
        inserted (c/add client (keyword id) (json/json-str metric))
        ]
    (assoc metric :id id :inserted inserted)
    )
  )

(defn insert-metrics [metrics]
  (map #(liberator-tutorial.db-couchbase/insert-metric %) metrics))


(defn retrieve-metric [id]
  (let [row (c/get-json client (keyword id))]
    (when-not (nil? row)
      (assoc row :id id))))

(defn retrieve-metrics [selection]
  (let [{:keys [host service key tags from to]} selection
        switch [(nil? host) (nil? service) (nil? key)]]
    (case switch
      [true false false] (retrieve-metrics-service-key service key from to)
      [false false false] (retrieve-metrics-host-service-key host service key from to)
      []
      )
    )
  )

(defn delete-metric [id]
  (when (c/delete client (keyword (:id%)))
    {:id id}))

(defn delete-metrics [selection]
  (let [rows-to-delete (retrieve-metrics selection)]
    (do
      (map #(c/delete client (keyword (:id %))) rows-to-delete)
      (map #(hash-map :id (:id %)) rows-to-delete)
      ))
  )


