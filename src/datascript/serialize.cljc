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
   (defmacro array
     "Platform-native array representation (java.util.List on JVM, Array on JS)"
     [& args]
     (if-cljs &env
       (list* 'js* (str "[" (str/join "," (repeat (count args) "~{}")) "]") args)
       `(java.util.List/of ~@args))))

#?(:clj
   (defmacro dict
     "Platform-native dictionary representation (java.util.Map on JVM, Object on JS)"
     [& args]
     (if-cljs &env
       (list* 'js* (str "{" (str/join "," (repeat (/ (count args) 2) "~{}:~{}")) "}") args)
       `(array-map ~@args))))

(defn- amap [f xs]
  #?(:clj
     (let [arr (java.util.ArrayList. (count xs))]
       (reduce (fn [idx x] (.add arr (f x)) (inc idx)) 0 xs)
       arr)
     :cljs
     (let [arr (js/Array. (count xs))]
       (reduce (fn [idx x] (arrays/aset arr idx (f x)) (inc idx)) 0 xs)
       arr)))

(defn- amap-indexed [f xs]
  #?(:clj
     (let [arr (java.util.ArrayList. (count xs))]
       (reduce (fn [idx x] (.add arr (f idx x)) (inc idx)) 0 xs)
       arr)
     :cljs
     (let [arr (js/Array. (count xs))]
       (reduce (fn [idx x] (arrays/aset arr idx (f idx x)) (inc idx)) 0 xs)
       arr)))

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
            left      (db/datom 0 attr nil)
            right     (db/datom db/emax nil nil)
            next-attr (:a (first (set/slice (:aevt db) left right attr-comparator)))]
        (if (some? next-attr)
          (recur (conj! attrs next-attr))
          (persistent! attrs))))))

(defn ^:export serializable
  "Converts db into a data structure (not string!) that can be fed to JSON
   serializer of your choice (`js/JSON.stringify` in CLJS, `cheshire.core/generate-string` or
   `jsonista.core/write-value-as-string` in CLJ).

   Options:

   Non-primitive values will be serialized using optional :freeze-fn (`pr-str` by default).

   :include-aevt?, :include-avet? Set to false to reduce snapshot size, but increase read times.
   Defaults to true.

   Serialized structure breakdown:

   count    :: number    
   tx0      :: number
   max-eid  :: number
   max-tx   :: number
   schema   :: freezed :schema
   attrs    :: [keywords ...]
   keywords :: [keywords ...]
   eavt     :: [[e a-idx v dtx] ...]
   a-idx    :: index in attrs
   v        :: (string | number | boolean | [0 <index in keywords>] | [1 <freezed v>])
   dtx      :: tx - tx0
   aevt     :: [<index in eavt> ...]
   avet     :: [<index in eavt> ...]"
  ([db] (serializable db {}))
  ([db {:keys [freeze-fn include-aevt? include-avet?]
        :or {freeze-fn     pr-str
             include-aevt? true
             include-avet? true}}]
   (let [attrs       (all-attrs db)
         attrs-map   (into {} (map vector attrs (range)))
         _           (db/log-time! "all-attrs")
         *kws        (volatile! (transient []))
         *kw-map     (volatile! (transient {}))
         write-kw    (fn [kw]
                       (let [idx (or
                                   (get @*kw-map kw)
                                   (let [keywords (vswap! *kws conj! kw)
                                         idx      (dec (count keywords))]
                                     (vswap! *kw-map assoc! kw idx)
                                     idx))]
                         (array 0 idx)))
         write-other (fn [v] (array 1 (freeze-fn v)))
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
                         (db/datom-set-idx d idx)
                         (let [e  (.-e d)
                               a  (attrs-map (.-a d))
                               v  (write-v (.-v d))
                               tx (- (.-tx d) db/tx0)]
                           (array e a v tx)))
                       (:eavt db))
         _           (db/log-time! "eavt")
         aevt        (when include-aevt?
                       (amap-indexed (fn [_ ^Datom d] (db/datom-get-idx d)) (:aevt db)))
         _           (db/log-time! "aevt")
         avet        (when include-avet?
                       (amap-indexed (fn [_ ^Datom d] (db/datom-get-idx d)) (:avet db)))
         _           (db/log-time! "avet")
         schema      (freeze-fn (:schema db))
         freeze-kw   str
         attrs       (amap freeze-kw attrs)
         kws         (amap freeze-kw (persistent! @*kws))]
       (dict
         "count"    (count (:eavt db))
         "tx0"      db/tx0
         "max-eid"  (:max-eid db)
         "max-tx"   (:max-tx db)
         "schema"   schema
         "attrs"    attrs
         "keywords" kws
         "eavt"     eavt
         "aevt"     aevt
         "avet"     avet))))
