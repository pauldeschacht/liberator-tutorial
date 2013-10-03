(ns liberator-tutorial.request-utils
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :refer (info error warn fatal)]
            [clojure.java.io :as io]
            
            [clj-time.core :as timecore]
            [clj-time.coerce :as timecoerce]

            )
  )

;; convert the body to a reader. Useful for testing in the repl
;; where setting the body to a string is much simpler.
(defn body-as-string [ctx]
  (if-let [body (get-in ctx [:request :body])]
    (condp instance? body
      java.lang.String body
      (slurp (io/reader body)))))


(defn create-period [str-period]
  (condp = str-period
    "monthly" [:year :month]
    "weekly"  [:year :week]
    "daily" [:year :month :day-in-month]
    [:year :month]))


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
; input: [{period1} [list of measures] {period2} [list of measures] ]
; output: [ {period1} aggregate1 {period2} aggregate2 ]
(defn aggregate-data-by-period [data aggregate-fn]
  (map #(hash-map (key %) (aggregate-fn (val %))) data))

; bring the value associated with a key (type of key is map) inside the key
; input: [{period} value1 {period} value2...]
;output: [{period :key-name value} {period2 :key-name value2} ]
(defn flatten-grouped-data [data key-name]
  (map #(assoc (first (keys %)) key-name (first (vals %))) data  ))


;
; different formatting tools (csv,json and xml)
;
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

(defn epoch-to-date [epoch]
  (let [dt (timecoerce/from-long epoch)
        year (timecore/year dt)
        month (timecore/month dt)
        dd (timecore/day dt)
        week (.getWeekyear dt)]
    {:year year :month month :day-in-month dd :week week}))

(defn epoch-to-date-rows [rows]
  (map #(merge % (epoch-to-date (* 1000 (:TIME %)))) rows))

(defn extract-metrics-from-rows [rows]
  (map #(:MEASURE %) rows))

