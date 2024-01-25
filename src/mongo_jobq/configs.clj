(ns mongo-jobq.configs
  (:require [mongo-jobq.datasources :as ds]
            [environ.core :refer [env]]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [taoensso.timbre :as timbre])
  (:import (java.net InetAddress)
           (java.util Date)))

(def host (.getHostName (InetAddress/getLocalHost)))

(def default-configs {:action (if (:dev env) "pause" "run")
                      :pause-wait 20
                      :empty-Q-wait 5
                      :num-consumers 3
                      :queries ["{}"]
                      :default-est-time 7200})
(def default-job-time 7200) ;update run-after flag by X seconds

(def configs (atom {}))
(def consumer-ids (atom []))
(def id->active-jobs (atom {}))
(def consumers-enabled? (atom true))

(def config-refresh-interval 5000)
(defn db [] (ds/get-db "jobq_configs"))

(def ^:dynamic *consumer-id* nil)

(defn refresh-configs!
  "Get current configs for this host from Mongo, also set heartbeat"
  []
  (let [old-configs @configs
        new-configs (merge default-configs
                           (mc/find-and-modify (db) "mongo_jobs" {:_id host}
                                               {"$set" {:last-heartbeat (new Date)}}
                                               {:return-new true :upsert true}))]
    (reset! configs new-configs)
    (when (not= (dissoc old-configs :last-heartbeat)
                (dissoc new-configs :last-heartbeat))
      (timbre/info (str "Config change - old configs: " (pr-str old-configs)
                        ", new configs: " (pr-str new-configs))))))

(defn init-configs!
  "Block until machine configs are read from Mongo. If they don't exist, the box should do no work until they are added."
  []
  (while (empty? @configs)
    (try (timbre/info (str "Reading initial worker configuration for host: " host))
         (refresh-configs!)
         (catch Exception e (timbre/error (str "Could not read initial worker configuration for host: " host "."
                                               " Trying again in " (int (/ config-refresh-interval 1000)) " seconds."))
                (timbre/error e)
                (Thread/sleep config-refresh-interval)))))

(defn poll-configs!
  "Periodically refresh configs from Mongo to pick up changes. First config read is blocking to prevent
   execution before necessary configs are set."
  []
  (init-configs!)
  (future (while true
            (try (Thread/sleep config-refresh-interval)
                 (refresh-configs!)
                 (catch Exception e
                   (timbre/error (str "Error while attempting to refresh config for host: " host "\n"))
                   (timbre/error e))))))
