(ns user
  (:require
   [cognitect.transit :as t]
   [datascript.core :as d]
   [datascript.db :as db]
   [datascript.transit :as dt]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]))

(def file (slurp "transit.edn"))

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

(defn -main []
  (bench file))