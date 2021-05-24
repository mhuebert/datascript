(ns user
  (:require
   [clojure.string :as str]
   [cognitect.transit :as t]
   [datascript.core :as d]
   [datascript.db :as db]
   [datascript.transit :as dt]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]))

(defn read-transit-str [s]
  (t/read (t/reader :json {}) s))

(defn write-transit-str [o]
  (t/write (t/writer :json {}) o))

(defn ^:export write-db [db]
  (db/start-bench!)
  (let [attrs     (distinct (map :a (:aevt db)))
        attrs-map (into {} (map vector attrs (range)))
        _         (db/log-time! "attrs")
        write-v   #(cond
                     (string? %) %
                     (number? %) %
                     (true? %)   %
                     (false? %)  %
                     :else #js [(write-transit-str %)])
        write-arr (fn [datoms]
                    (let [arr (arrays/make-array (count datoms))
                          *i  (atom 0)]
                      (doseq [d datoms]
                        (arrays/aset arr @*i #js [(:e d) (attrs-map (:a d)) (write-v (:v d)) (- (:tx d) db/tx0)])
                        (swap! *i inc))
                      arr))
        eavt      (write-arr (:eavt db))
        _         (db/log-time! "eavt")
        aevt      (write-arr (:aevt db))
        _         (db/log-time! "aevt")
        avet      (write-arr (:avet db))
        _         (db/log-time! "avet")
        json      #js {"schema" (write-transit-str (:schema db))
                       "attrs"  (arrays/into-array (map str attrs))
                       "tx0"    db/tx0
                       "count"  (count (:eavt db))
                       "eavt"   eavt
                       "aevt"   aevt
                       "avet"   avet}
        res       (js/JSON.stringify json #_#_nil 2)
        _         (db/log-time! "JSON.stringify")]
    (println (db/pad-left (- (db/now) @db/*t-start)) "ms " "TOTAL write-db" (count (:eavt db)) "datoms ->" (count res) "bytes")
    res))

(defn ^:export read-db [s]
  (db/start-bench!)
  (let [json     (js/JSON.parse s)
        _        (db/log-time! "JSON.parse")
        schema   (read-transit-str (aget json "schema"))
        attrs    (mapv #(if (str/starts-with? % ":") (keyword (subs % 1)) %) (aget json "attrs"))
        tx0      (aget json "tx0")
        read-arr (fn [json key]
                   (let [arr (aget json key)]
                     (delay
                       (dotimes [i (alength arr)]
                         (let [da  (aget arr i)
                               e   (aget da 0)
                               a   (nth attrs (aget da 1))
                               v   (aget da 2)
                               v   (if (arrays/array? v)
                                     (read-transit-str (aget v 0))
                                     v)
                               tx  (+ tx0 (aget da 3))]
                           (aset arr i (db/datom e a v tx))))
                       (db/log-time! (str key " arrays -> datoms"))
                       arr)))
        eavt   (read-arr json "eavt")
        aevt   (read-arr json "aevt")
        avet   (read-arr json "avet")
        db     (db/init-db eavt aevt avet schema)]
    (println (db/pad-left (- (db/now) @db/*t-start)) "ms " "TOTAL read-db" (count s) "bytes ->" (count (:eavt db)) "datoms")
    db))

(defn ^:export bench [file]
  (db/start-bench!)
  (def db "datascript.transit/read" (dt/read-transit-str file))
  (println (db/pad-left (- (db/now) @db/*t-start)) "ms " "TOTAL datascript.transit/read-transit-str")

  ; (db/start-bench!)
  ; (dt/write-transit-str db)
  ; (db/log-time! "TOTAL datascript.transit/write-transit-str")

  db)