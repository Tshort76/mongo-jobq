(ns mongo-jobq.core
  (:require [analytics.metrics :as metrics]
            [mongo-jobq.configs :as configs]
            [mongo-jobq.supervisor :as supervisor]
            [monger.collection :as mc]
            [mongo-jobq.datasources :as ds]
            [taoensso.timbre :as timbre]
            [util.metrics :as machine-metrics])
  (:gen-class)
  (:import (java.util Date)))

(defn fix-dropped-jobs []
  (timbre/info "Looking for jobs dropped by last shutdown...")
  ;Mongo doesn't let you query for a match on the last element of a vector, so we do an any query and then filter
  (let [maybe-dropped-jobs (mc/find-maps (ds/get-db "mongo_jobq_configs") "queue" {"start-times.host" configs/host})
        definitely-dropped-jobs (filter #(= configs/host (-> % :start-times last :host)) maybe-dropped-jobs)]
    (timbre/info (str "Found " (count definitely-dropped-jobs) " incomplete jobs leftover from the last shutdown."))
    (doseq [job definitely-dropped-jobs]
      (timbre/info (str "Updating job affected by last service shut-down to be run again. Job ID: " (:_id job)))
      (mc/update-by-id (ds/get-db "mongo_jobq_configs") "queue" (:_id job) (assoc job :run-after (Date.))))))

(defn launch []
  (future
    (timbre/debug (str "mongo-jobq is starting up on host: " configs/host))
    (metrics/init-metric-config)
    (machine-metrics/periodically-report-machine-stats 5)
    (fix-dropped-jobs)
    (configs/poll-configs!)
    (supervisor/supervise!)
    (shutdown-agents)))
