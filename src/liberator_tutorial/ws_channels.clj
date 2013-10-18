(ns liberator-tutorial.ws-channels
  (:require [org.httpkit.server :as httpkit]
            [clojure.tools.logging :refer (info error warn fatal)]
            )

  )

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


