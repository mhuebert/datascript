(ns user
  (:require
   [clojure.string :as str]
   [cognitect.transit :as t]
   [com.cognitect.transit :as transit-js]
   [datascript.core :as d]
   [datascript.db :as db :refer [cond+]]
   [datascript.transit :as dt]
   [goog.object :as gobject]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]))

(defn read-transit-str [s]
  (t/read (t/reader :json {}) s))

(defn write-transit-str [o]
  (t/write (t/writer :json {}) o))

(defn read-transit-json [o]
  (let [reader (t/reader :json {})
        handlers #js {"$"    (fn [v] (symbol v))
                      ":"    (fn [v] (keyword v))
                      "set"  (fn [v] (into #{} v))
                      "list" (fn [v] (into () (.reverse v)))
                      "cmap" (fn [v]
                               (loop [i 0 ret (transient {})]
                                 (if (< i (alength v))
                                   (recur (+ i 2)
                                     (assoc! ret (aget v i) (aget v (inc i))))
                                   (persistent! ret))))
                      "with-meta" (fn [v] (with-meta (aget v 0) (aget v 1)))}
        opts  #js {"handlers"       handlers
                   "mapBuilder"     (t/MapBuilder.)
                   "arrayBuilder"   (t/VectorBuilder.)
                   "prefersStrings" false}
        cache (transit-js/readCache)]
    (.decode (transit-js/decoder opts) o cache)))

(defn write-transit-json [o]
  (let [writer (t/writer :json)]
    (.write writer o #js {:marshalTop false})))

(defn ^:export write-db-v1 [db]
  (db/start-bench!)
  (let [attrs     (distinct (map :a (:aevt db)))
        attrs-map (into {} (map vector attrs (range)))
        _         (db/log-time! "attrs")
        write-v   #(cond
                     (string? %) %
                     (number? %) %
                     (true? %)   %
                     (false? %)  %
                     :else #js [(write-transit-json %)])
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
        json      #js {"schema" (write-transit-json (:schema db))
                       "attrs"  (arrays/into-array (map str attrs))
                       "tx0"    db/tx0
                       "count"  (count (:eavt db))
                       "eavt"   eavt
                       "aevt"   aevt
                       "avet"   avet}
        res       (js/JSON.stringify json #_#_nil 2)
        _         (db/log-time! "JSON.stringify")]
    (println (db/pad-left (- (db/now) @db/*t-start)) "ms " "TOTAL write-db-v1" (count (:eavt db)) "datoms ->" (count res) "bytes")
    res))

(defn ^:export write-db-v2 [db]
  (db/start-bench!)
  (let [attrs     (distinct (map :a (:aevt db)))
        attrs-map (into {} (map vector attrs (range)))
        _         (db/log-time! "attrs")
        write-v   #(cond
                     (string? %) %
                     (number? %) %
                     (true? %)   %
                     (false? %)  %
                     :else #js [(write-transit-json %)])
        write-arr (fn [datoms]
                    (let [arr (arrays/make-array (count datoms))
                          *i  (atom 0)]
                      (doseq [d datoms]
                        (arrays/aset arr @*i #js [(:e d) (attrs-map (:a d)) (write-v (:v d)) (- (:tx d) db/tx0)])
                        (swap! *i inc))
                      arr))
        avet        (write-arr (:avet db))
        _           (db/log-time! "avet")
        indexed     (:db/index (:rschema db))
        not-indexed (write-arr (->> (:eavt db) (remove (fn [^Datom d] (indexed (.-a d))))))
        _           (db/log-time! "not-indexed")
        json        #js {"schema"      (write-transit-json (:schema db))
                         "attrs"       (arrays/into-array (map str attrs))
                         "tx0"         db/tx0
                         "count"       (count (:eavt db))
                         "avet"        avet
                         "not_indexed" not-indexed}
        res         (js/JSON.stringify json #_#_nil 2)
        _           (db/log-time! "JSON.stringify")]
    (println (db/pad-left (- (db/now) @db/*t-start)) "ms " "TOTAL write-db-v2" (count (:eavt db)) "datoms ->" (count res) "bytes")
    res))

(defn read-datom [datom-array attrs tx0]
  (let [e   (aget datom-array 0)
        a   (nth attrs (aget datom-array 1))
        v   (aget datom-array 2)
        v   (if (arrays/array? v)
              (read-transit-json (aget v 0))
              v)
        tx  (+ tx0 (aget datom-array 3))]
    (db/datom e a v tx)))

(defn read-datoms [msg datoms-array attrs tx0]
  (dotimes [i (alength datoms-array)]
    (aset datoms-array i (read-datom (aget datoms-array i) attrs tx0)))
  (db/log-time! (str "read-datoms " msg))
  datoms-array)

(defn ^:export test-sorting [s]
  (db/start-bench!)
  (let [json   (js/JSON.parse s)
        _      (db/log-time! "JSON.parse")
        attrs  (mapv #(if (str/starts-with? % ":") (keyword (subs % 1)) %) (aget json "attrs"))
        tx0    (aget json "tx0")
        eavt   (aget json "eavt")
        datoms (read-datoms "eavt" (arrays/aclone eavt) attrs tx0)
        _      (.sort datoms db/cmp-datoms-aevt-quick)
        _      (db/log-time! "sort datoms db/cmp-datoms-aevt-quick")
        _      (.sort datoms db/cmp-datoms-eavt-quick)
        _      (db/log-time! "sort datoms db/cmp-datoms-eavt-quick")
        array  (arrays/aclone eavt)
        _      (db/log-time! "aclone")
        _      (.sort array
                 (fn [a b]
                   (cond+
                     :let [cmp-a (- (aget a 1) (aget b 1))]
                     (not (== 0 cmp-a)) cmp-a

                     :let [cmp-e (- (aget a 0) (aget b 0))]
                     (not (== 0 cmp-e)) cmp-e

                     :let [cmp-v (db/value-compare (aget a 2) (aget b 2))]
                     (not (== 0 cmp-v)) cmp-v

                     :let [cmp-tx (- (aget a 3) (aget b 3))]
                     (not (== 0 cmp-tx)) cmp-tx)))
        _      (db/log-time! "sort arrays")
        array  (.map (arrays/aclone eavt) (fn [a] #js {"e" (aget a 0) "a" (aget a 1) "v" (aget a 2) "tx" (aget a 3)}))
        _      (db/log-time! "aclone")
        _      (.sort array
                 (fn [a b]
                   (cond+
                     :let [cmp-a (- (aget a "a") (aget b "a"))]
                     (not (== 0 cmp-a)) cmp-a

                     :let [cmp-e (- (aget a "e") (aget b "e"))]
                     (not (== 0 cmp-e)) cmp-e

                     :let [cmp-v (db/value-compare (aget a "v") (aget b "v"))]
                     (not (== 0 cmp-v)) cmp-v

                     :let [cmp-tx (- (aget a "tx") (aget b "tx"))]
                     (not (== 0 cmp-tx)) cmp-tx)))
        _      (db/log-time! "sort objects")
        ]
    nil))

(defn ^:export read-db-v1 [s]
  (db/start-bench!)
  (let [json     (js/JSON.parse s)
        _        (db/log-time! "JSON.parse")
        schema   (read-transit-json (aget json "schema"))
        attrs    (mapv #(if (str/starts-with? % ":") (keyword (subs % 1)) %) (aget json "attrs"))
        tx0      (aget json "tx0")
        read-arr (fn [json key]
                   (let [arr (aget json key)]
                     (delay
                       (read-datoms key arr attrs tx0))))
        eavt   (read-arr json "eavt")
        aevt   (read-arr json "aevt")
        avet   (read-arr json "avet")
        db     (db/init-db eavt aevt avet schema)]
    (println (db/pad-left (- (db/now) @db/*t-start)) "ms " "TOTAL read-db-v1" (count s) "bytes ->" (count (:eavt db)) "datoms")
    db))

(defn ^:export read-db-v2 [s]
  (db/start-bench!)
  (let [json        (js/JSON.parse s)
        _           (db/log-time! "JSON.parse")
        schema      (read-transit-json (aget json "schema"))
        attrs       (mapv #(if (str/starts-with? % ":") (keyword (subs % 1)) %) (aget json "attrs"))
        tx0         (aget json "tx0")
        avet        (read-datoms "avet" (aget json "avet") attrs tx0)
        not-indexed (read-datoms "not_indexed" (aget json "not_indexed") attrs tx0)
        aevt        (arrays/aconcat avet not-indexed)
        _           (db/log-time! "aconcat avet not-indexed")
        _           (arrays/asort aevt db/cmp-datoms-aevt-quick)
        _           (db/log-time! "asort aevt")
        eavt        (arrays/aclone aevt)
        _           (db/log-time! "aclone eavt")
        _           (arrays/asort eavt db/cmp-datoms-eavt-quick)
        _           (db/log-time! "asort eavt")
        db          (db/init-db (delay eavt) (delay aevt) (delay avet) schema)]
    (println (db/pad-left (- (db/now) @db/*t-start)) "ms " "TOTAL read-db-v2" (count s) "bytes ->" (count (:eavt db)) "datoms")
    db))

(defn ^:export bench [filename slurp spit]
  (db/start-bench!)
  (let [file (slurp filename)
        _    (db/log-time! (str "Read " filename " length = " (count file) " bytes"))
        _    (def db (read-db-v1 file))
        [_ basename] (re-matches #"(.*)\.[^.]+" filename)
        s    (write-db-v2 db)
        _    (spit (str basename "_roundtrip.json") s)
        _    (db/log-time! (str "Write " (str basename "_roundtrip.json") " length = " (count s) " bytes"))
        _    (read-db-v2 s)]
    'DONE))