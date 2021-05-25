(ns user
  (:require
   [cognitect.transit :as t]
   [datascript.core :as d]
   [datascript.db :as db]
   [datascript.transit :as dt]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]))

; (def file (slurp "transit.edn"))

(defn ^:export bench [file]
  (db/start-bench!)
  (def db "datascript.transit/read" (dt/read-transit-str file))
  (println (db/pad-left (- (db/now) @db/*t-start)) "ms " "TOTAL datascript.transit/read-transit-str")

  (db/start-bench!)
  (dt/write-transit-str db)
  (db/log-time! "TOTAL datascript.transit/write-transit-str")

  db)

(defn stats [db]
  (let [keys (into #{} (map :a) (:eavt db))]
    (->>
      (for [k keys
            :let [datoms (d/datoms db :aevt k)]]
        [k [(type (:v (first datoms))) (count datoms) #_(mapv :v (take 10 datoms))]])
      (sort-by #(second (second %)))
      (reverse))))

(defn -main [file]
  (bench (slurp file))
  :done)

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