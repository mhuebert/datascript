(ns user
  (:require
   [cognitect.transit :as t]
   [cheshire.core :as cheshire]
   [datascript.core :as d]
   [datascript.db :as db]
   [datascript.serialize :as ds]
   [datascript.transit :as dt]
   [jsonista.core :as jsonista]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]))

; (def file (slurp "transit.edn"))

(defn ^:export bench [filename]
  (let [[_ basename] (re-matches #"(.*)\.[^.]+" filename)
        file    (slurp filename)
        _       (db/start-bench!)
        db      (dt/read-transit-str file)
        _       (db/log-time-from-start! (str "TOTAL datascript.transit/read-transit-str " (count (:eavt db)) "/" (count (:aevt db)) "/" (count (:avet db)) " datoms"))

        ; _       (db/start-bench!)
        ; transit (dt/write-transit-str db)
        ; _       (db/log-time! "TOTAL datascript.transit/write-transit-str")

        _            (db/start-bench!)
        serializable (ds/serializable db)
        _            (db/log-time-from-start! "TOTAL ds/serializable")

        ; json         (cheshire/generate-string serializable {:pretty true})
        ; _            (db/log-time! (str "cheshire/generate-string " (count json) " bytes"))

        json         (jsonista/write-value-as-string serializable)
        _            (db/log-time! (str "jsonista/write-value-as-string " (count json) " bytes"))

        ; _            (dotimes [i 10]
        ;                (db/start-bench!)
        ;                (jsonista/write-value-as-string (ds/serializable db))
        ;                (db/log-time-from-start! "serializable + json"))
        
        ; _            (db/start-bench!)
        ; json         (jsonista/write-value-as-string (ds/serializable db {:include-aevt? false}))
        ; _            (db/log-time-from-start! (str "jsonista/write-value-as-string :include-aevt? false " (count json) " bytes"))

        ; _            (db/start-bench!)
        ; json         (jsonista/write-value-as-string (ds/serializable db {:include-aevt? false, :include-avet? false}))
        ; _            (db/log-time-from-start! (str "jsonista/write-value-as-string :include-aevt? false, :include-avet? false " (count json) " bytes"))

        ; _            (db/start-bench!)
        ; json         (jsonista/write-value-as-string (ds/serializable db {:freeze-fn dt/write-transit-str}))
        ; _            (db/log-time-from-start! (str "jsonista/write-value-as-string :freeze-fn dt/write-transit-str " (count json) " bytes"))
        
        ; transit      (dt/write-transit-str serializable)
        ; _            (db/log-time! (str "dt/write-transit-str " (count transit) " bytes"))

        serializable'  (jsonista/read-value json)
        _              (db/log-time! (str "jsonista/read-value " (count json) " bytes"))
        db'            (ds/from-serializable serializable')
        _              (db/log-time! (str "ds/from-serializable " (count (:eavt db')) "/" (count (:aevt db')) "/" (count (:avet db')) " datoms"))

        serializable'  (jsonista/read-value json (com.fasterxml.jackson.databind.ObjectMapper.))
        _              (db/log-time! (str "jsonista/read-value + mapper " (count json) " bytes"))
        db'            (ds/from-serializable serializable')
        _              (db/log-time! (str "ds/from-serializable " (count (:eavt db')) "/" (count (:aevt db')) "/" (count (:avet db')) " datoms"))

        _              (db/start-bench!)
        json           (-> db
                         (ds/serializable)
                         (jsonista/write-value-as-string))
        _              (db/log-time! "write")
        db'            (-> json
                         (jsonista/read-value)
                         (ds/from-serializable))
        _              (db/log-time! "read")
        _              (db/log-time-from-start! (str "roundtrip edn " (count json) " bytes, " (count (:eavt db')) " / " (count (:aevt db')) " / " (count (:avet db')) " datoms"))

        _              (db/start-bench!)
        json           (-> db
                         (ds/serializable {:freeze-fn dt/write-transit-str})
                         (jsonista/write-value-as-string))
        _              (db/log-time! "write")
        db'            (-> json
                         (jsonista/read-value)
                         (ds/from-serializable {:thaw-fn dt/read-transit-str}))
        _              (db/log-time! "read")
        _              (db/log-time-from-start! (str "roundtrip transit " (count json) " bytes, " (count (:eavt db')) " / " (count (:aevt db')) " / " (count (:avet db')) " datoms"))
]

    'DONE))

(defn stats [db]
  (let [keys (into #{} (map :a) (:eavt db))]
    (->>
      (for [k keys
            :let [datoms (d/datoms db :aevt k)]]
        [k [(type (:v (first datoms))) (count datoms) #_(mapv :v (take 10 datoms))]])
      (sort-by #(second (second %)))
      (reverse))))

(defn -main [filename]
  (bench filename))

(def ^:dynamic *dict)

(defn anonymize-string [s]
  (if (<= (count s) 2)
    s
    (if-some [r (get @*dict s)]
      r
      (let [r (clojure.string/replace s #"[0-9a-zA-Z]"
                (fn [c]
                  (let [ch (.codePointAt c 0)]
                    (cond
                      (<= (int \0) ch (int \9)) (str (rand-nth "0123456789"))
                      (<= (int \a) ch (int \z)) (str (rand-nth "qwertyuiopasdfghjklzxcvbnm"))
                      (<= (int \A) ch (int \Z)) (str (rand-nth "QWERTYUIOPASDFGHJKLZXCVBNM"))
                      :else c))))]
        (swap! *dict assoc s r)
        r))))

(defn anonymize-primitive [p]
  (cond
    (string? p)  (anonymize-string p)
    (boolean? p) (rand-nth [true false])
    (number? p)  p
    (keyword? p) (-> (str p) anonymize-string (subs 1) keyword)
    :else nil))

(defn anonymize-obj [o]
  (clojure.walk/postwalk
    (fn [form]
      (or (anonymize-primitive form) form))
    o))

(defn anonymize-db [db]
  (binding [*dict (atom {})]
    (let [db' (d/db-with db
                [[:db/add 63490 :user/uid "CHxhvJ7VwhSEeJI217QsbgO8hpu2"]
                 [:db/add 63491 :user/uid "CHxhvJ7VwhSEeJI217QsbgO8hpu3"]
                 [:db/add 63492 :user/uid "CHxhvJ7VwhSEeJI217QsbgO8hpu4"]
                 [:db/add 63493 :user/uid "CHxhvJ7VwhSEeJI217QsbgO8hpu5"]])
          txs (for [d (:eavt db')]
                [:db/add (:e d) (:a d) (anonymize-obj (:v d)) (:tx d)])]
      (d/db-with (d/empty-db (:schema db)) txs))))

(defn minify-db [db factor]
  (d/db-with (d/empty-db (:schema db))
    (for [[d _] (partition-all factor (:eavt db))]
      [:db/add (:e d) (:a d) (:v d) (:tx d)])))

(comment
  (def db' (anonymize-db db))
  (spit "db_3M.transit" (dt/write-transit-str db'))
  (def db'' (minify-db db' 10))
  (spit "db_300k.transit" (dt/write-transit-str db''))
  (def db'' (minify-db db' 100))
  (spit "db_30k.transit" (dt/write-transit-str db''))
)