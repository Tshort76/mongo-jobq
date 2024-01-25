(ns mongo-jobq.datasources
  (:require [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [clojure.walk :as walk]
            [environ.core :refer [env]]
            [monger.collection :as mc]
            [monger.core :as mg]
            [monger.result :as mr]
            [monger.conversion :refer [ConvertFromDBObject ConvertToDBObject] :as mconv]
            [taoensso.timbre :as log]
            [clojure.java.io :as io])
  (:import (com.github.fakemongo Fongo)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.util Date)
           (org.bson.types ObjectId)
           (java.util.concurrent TimeUnit)
           (com.mongodb MongoCommandException)))

;==========Extend mongo protocol===================
(def prefix "bigdec:")
(def prefix-count (count prefix))

(defn big-dec-serialize [value]
  (str prefix value))

(defn big-dec-deserialize [s]
  (if (and (> (count s) prefix-count)
           (= (subs s 0 prefix-count) prefix))
    (try
      (bigdec (subs s prefix-count (count s)))
      (catch Exception _
        s))
    s))

(extend-protocol ConvertToDBObject
  BigDecimal
  (to-db-object [^BigDecimal nr]
    (big-dec-serialize nr)))

(extend-protocol ConvertFromDBObject
  String
  (from-db-object [^String input _]
    (big-dec-deserialize input)))

;=====================================

;Shamelessly copied from monger: https://github.com/michaelklishin/monger/blob/master/src/clojure/monger/collection.clj#L196
;Added ability to access timeout, along with a default timeout of half a second
(defn find-and-modify
  "Atomically modify a document (at most one) and return it."
  ([db coll conditions document {:keys [fields sort remove return-new upsert keywordize timeout]
                                 :or {fields nil sort nil remove false return-new false
                                      upsert false keywordize true timeout 500}}]
   (let [coll (.getCollection db (name coll))
         maybe-fields (when fields (mconv/as-field-selector fields))
         maybe-sort (when sort (mconv/to-db-object sort))]
     (mconv/from-db-object
      (.findAndModify coll (mconv/to-db-object conditions) maybe-fields maybe-sort remove
                      (mconv/to-db-object document) return-new upsert timeout TimeUnit/MILLISECONDS)
      keywordize))))

(defn- ensure-indices [db [coll {indices :indices}]]
  (run! #(try
           (log/trace (str "ensuring index: " (with-out-str (pprint %))))
           ;NOTE - even if the index passed here is set to be applied in the background, the
           ; following call is a blocking call, and if it takes a while to apply a the index,
           ; you may get a timeout during init of your repl session.
           (apply mc/ensure-index db (name coll) %)
           (catch MongoCommandException mce
             (if-let [filename (and (== 85 (.getErrorCode mce))
                                    (second (re-find #"Index with name: (\S+?) already exists with different options" (.getErrorMessage mce))))]
               (do
                 (log/trace (str "dropping index: " (with-out-str (pprint %))))
                 (mc/drop-index db (name coll) filename)
                 (log/trace (str "ensuring index: " (with-out-str (pprint %))))
                 (apply mc/ensure-index db (name coll) %))
               (throw mce))))
        indices))

(defn- sync-db [conn {:keys [db-name collections]}]
  (let [db (mg/get-db conn db-name)]
    (run! #(ensure-indices db %) collections)))

(defn- sync-mongo-dbs [conn]
  (try
    (let [db-configs (edn/read-string (slurp (clojure.java.io/resource "indices/mongo_indices.edn")))]
      (when conn
        (do
          (log/trace "syncing mongo indices...")
          (run! #(sync-db conn %) db-configs))))
    (catch Exception e (log/warn (.getMessage e)))))

(defn fongo-connection []
  (let [db (Fongo. "mongo server 1")
        conn (.getMongo db)
        tmpdir (or (env :fongo-source-files-filepath)
                   (str (Files/createTempDirectory "fongo-files" (into-array FileAttribute []))))]
    (log/trace (str "Fongo will use \"" tmpdir "\" for file storage."))
    (when-not (env :fongo-source-files-filepath)
      (log/debug (str "The Fongo directory can be set with the environment variable 'fongo-source-files-filepath'")))
    (sync-mongo-dbs conn)
    {:connection-type :fongo
     :db db
     :conn conn
     :ro-conn conn
     :source-files-filepath tmpdir}))

(defn connect-via-config [{:keys [mongo-host mongo-user-db mongo-password mongo-user-name source-files-filepath replicaset-name]}]
  (let [connection-str (str "mongodb://" mongo-user-name ":" mongo-password "@" mongo-host "/" mongo-user-db)
        read-secondary-connection-str (str connection-str "?"
                                           (if (not-empty replicaset-name) (str "?replicaSet=" replicaset-name "&") "")
                                           "readPreference=secondary")
        log-str (str "mongodb://" mongo-user-name ":******@" mongo-host "/" mongo-user-db)]
    (try
      (do
        (let [conn-map (do
                         (log/info (str "Attempting to connect to : " log-str))
                         (mg/connect-via-uri connection-str))
              {read-secondary-conn :conn} (mg/connect-via-uri read-secondary-connection-str)]
          (when-not (env :disable-auto-mongo-index-creation)
            (-> conn-map :conn sync-mongo-dbs))
          (assoc conn-map :source-files-filepath source-files-filepath
                 :read-secondary-conn read-secondary-conn
                 :connection-type :mongo)))
      (catch Exception e (log/warn e "error while connecting to mongo")))))

(defn- get-connection [{:keys [fongo] :as info}]
  (if fongo
    (fongo-connection)
    (let [required-mongo-keys [:mongo-host :mongo-user-db :mongo-password :mongo-user-name]]
      (if (every? info required-mongo-keys)
        (connect-via-config info)
        (log/warn (str "Missing the following env keys that are required to establish mongodb connections:"
                       (filterv (complement info) required-mongo-keys)))))))

(defn- connect-with-env!
  "Connect to a datasource using the environment."
  []
  (or (get-connection env) (log/error "Failed to establish connection to Mongo or Fongo.")))

(def ^{:dynamic true :private true} connection
  (delay (connect-with-env!)))

(defn base-path
  "This tells the program where to store files."
  []
  (:source-files-filepath @connection))

(defn get-db [db-name]
  (if db-name
    (let [{:keys [conn]} @connection]
      (when conn (mg/get-db conn db-name)))
    (log/warn "No database name provided.")))

(defn get-db-secondary-preferred [db-name]
  (if (not-empty db-name)
    (let [{:keys [read-secondary-conn]} @connection]
      (if read-secondary-conn
        (mg/get-db read-secondary-conn db-name)
        (log/info (str "attempting to access db: " db-name " with readPreference=secondary connection, but no such connection exists."))))
    (log/warn "No database name provided.")))

(defonce OID_LOCK (Object.))

(defn- get-sequential-object-ids [n]
  (let [ts (Date.)]
    (locking OID_LOCK (doall (repeatedly n #(ObjectId. ts))))))

(defn bulk-write [db coll documents]
  (when (seq documents)
    (let [ids (get-sequential-object-ids (count documents))
          docs (map (fn [doc id] (assoc doc :_id id)) documents ids)]
      (when (mr/acknowledged? (mc/insert-batch db coll docs)) ids))))

(defn mongo-working? []
  (try
    (mg/command (get-db "mongo_jobq_configs") {:ping 1})
    (catch Exception _ false)))

;; source: https://gist.github.com/edw/5128978
(defn delete-directory [directory-path-str]
  (doseq [file (-> directory-path-str
                   io/file
                   file-seq
                   reverse)]
    (io/delete-file file true)))

(defmacro with-fongo
  "Wrapper to swap all mongo calls to use fongo instead.
  Forces evaluation of all lazy-seqs returned."
  [& forms]
  `(let [conn# (delay (fongo-connection))]
     (with-bindings {#'connection conn#}
       (try
         (walk/prewalk identity (do ~@forms)) ;; prewalk forces all lazy-seqs to evaluate
         (finally (delete-directory (:source-files-filepath @conn#)))))))

;todo check that the commented lines don't do bad things then re-add them.
(defn no-mongo-wrapper
  "Wrapper to prevent all mongo calls.
  Forces evaluation of all lazy-seqs returned."
  [f & args]
  (with-bindings {#'connection (delay (throw (RuntimeException. "Attempted to read from database in pure job.  See no-monger-wrapper.")))}
    (->> args
         ; (walk/prewalk identity)
         (apply f))))
         ; (walk/prewalk identity)))) ;; prewalk forces all lazy-seqs to evaluate
