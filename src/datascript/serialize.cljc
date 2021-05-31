(ns datascript.serialize
  (:refer-clojure :exclude [amap])
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [datascript.db :as db #?(:cljs :refer-macros :clj :refer) [raise cond+] #?@(:cljs [:refer [Datom]])]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.arrays :as arrays])
  #?(:cljs (:require-macros [datascript.serialize :refer [array dict]]))
  #?(:clj
     (:import
       [datascript.db Datom])))

(defn- if-cljs [env then else]
  (if (:ns env) then else))

#?(:clj
  (defmacro array [& args]
    (if-cljs &env
      (->
        (list* 'js* (str "[" (str/join "," (repeat (count args) "~{}")) "]") args)
        (vary-meta assoc :tag 'array))
      `(vector ~@args))))

#?(:clj
  (defmacro dict [& args]
    (if-cljs &env
      (list* 'js* (str "{" (str/join "," (repeat (/ (count args) 2) "~{}:~{}")) "}") args)
      `(array-map ~@args))))

(defn- attr-comparator
  "Looks for a datom with attribute exactly bigger than the given one"
  [^Datom d1 ^Datom d2]
  (cond 
    (nil? (.-a d2)) -1
    (<= (compare (.-a d1) (.-a d2)) 0) -1
    true 1))

(defn- all-attrs
  "All attrs in a DB, distinct, sorted"
  [db]
  (if (empty? (:aevt db))
    []
    (loop [attrs (transient [(:a (first (:aevt db)))])]
      (let [attr      (nth attrs (dec (count attrs)))
            left      (db/datom nil attr nil)
            right     (db/datom nil nil nil)
            next-attr (:a (first (set/slice (:aevt db) left right attr-comparator)))]
        (if (some? next-attr)
          (recur (conj! attrs next-attr))
          (persistent! attrs))))))

(defn- amap [f xs]
  #?(:clj
     (mapv f xs)
     :cljs
     (let [arr (js/Array. (count xs))]
       (reduce (fn [a x] (.push a (f x)) a) arr xs))))

(defn- amap-indexed [f xs]
  #?(:clj
     (mapv f (range) xs)
     :cljs
     (let [arr (arrays/make-array (count xs))]
       (reduce (fn [idx x] (arrays/aset arr idx (f idx x)) (inc idx)) 0 xs)
       arr)))

(defn ^:export serializable
  "Converts db into a data structure (not string!) that can be fed to JSON
   serializer of your choice (`js/JSON.stringify` in CLJS, `cheshire.core/generate-string` or
   `jsonista.core/write-value-as-string` in CLJ).

   Non-primitive values will be serialized using optional :freeze-fn (pr-str by default).

   count    :: number    
   tx0      :: number
   max-tx   :: number
   schema   :: freezed :schema
   attrs    :: [keywords ...]
   keywords :: [keywords ...]
   eavt     :: [[e a-idx v dtx] ...]
   a-idx    :: index in attrs
   v        :: (string | number | boolean | {\"k\" <index in keywords>} | [<freezed-v>])
   dtx      :: tx - tx0
   aevt     :: [<index in eavt> ...]
   avet     :: [<index in eavt> ...]"
  ([db] (serializable db {:freeze-fn pr-str}))
  ([db {:keys [freeze-fn]}]
   (let [attrs       (all-attrs db)
         attrs-map   (into {} (map vector attrs (range)))
         *kws        (volatile! (transient []))
         *kw-map     (volatile! (transient {}))
         write-kw    (fn [kw]
                       (let [idx (or
                                   (get @*kw-map kw)
                                   (let [keywords (vswap! *kws conj! kw)
                                         idx      (dec (count keywords))]
                                     (vswap! *kw-map assoc! kw idx)
                                     idx))]
                         (dict "k" idx)))
         write-other (fn [v] (array (freeze-fn v)))
         write-v     (fn [v]
                       (cond
                         (string? v)  v
                         #?@(:clj [(ratio? v) (write-other v)])
                         (number? v)  v
                         (boolean? v) v
                         (keyword? v) (write-kw v)
                         :else        (write-other v)))
         eavt        (amap-indexed
                       (fn [idx ^Datom d]
                         (set! (.-idx d) idx)
                         (let [e  (.-e d)
                               a  (attrs-map (.-a d))
                               v  (write-v (.-v d))
                               tx (- (.-tx d) db/tx0)]
                           (array e a v tx)))
                       (:eavt db))
         aevt        (amap-indexed (fn [_ ^Datom d] (.-idx d)) (:aevt db))
         avet        (amap-indexed (fn [_ ^Datom d] (.-idx d)) (:avet db))
         schema      (freeze-fn (:schema db))
         freeze-kw   str
         attrs       (amap freeze-kw attrs)
         kws         (amap freeze-kw (persistent! @*kws))]
       (dict
         "count"    (count (:eavt db))
         "tx0"      db/tx0
         "max-tx"   (:max-tx db)
         "schema"   schema
         "attrs"    attrs
         "keywords" kws
         "eavt"     eavt
         "aevt"     aevt
         "avet"     avet))))
