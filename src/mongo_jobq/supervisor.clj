(ns mongo-jobq.supervisor
  (:require [clj-time.core :as t]
            [mongo-jobq.consumer :as c]
            [mongo-jobq.job-execution :as je]
            [mongo-jobq.configs :as configs]
            [taoensso.timbre :as timbre]
            [monger.collection :as mc]
            [clojure.pprint :as pp])
  (:import (java.util UUID)
           (org.bson.types ObjectId)))

(def sleep-time 5000) ;milliseconds

(defn cancel-job! [[_id {:keys [future cancel-source]}]]
  (if-let [old-job (mc/find-map-by-id (configs/db) "queue" _id)]
    (do (timbre/debug (str "Cancelling job: " _id " due to: " cancel-source))
        (future-cancel future)
        (je/complete-job (assoc old-job :cancellation-reason cancel-source
                                :start-time (->> old-job :start-times last :time)) nil)) ;TODO: How to handle start-time
    (timbre/error (str "Attempted to cancel job, but job-doc was absent for id: " _id))))

(defn manual-cancel-job! [job-id]
  (let [_id (cond (string? job-id) (ObjectId. job-id)
                  (= (type job-id) org.bson.types.ObjectId) job-id)]
    (timbre/info (str "Attempting manual cancellation of job with id: " job-id))
    (if-let [active-job (get @configs/id->active-jobs _id)]
      (cancel-job! [_id (assoc active-job :cancellation-reason :manual-request)])
      (timbre/debug (str "Could not cancel active job with id: " _id " as that job is not currently found in this worker's active-jobs:\n"
                         (with-out-str (pp/pprint @configs/id->active-jobs)))))))

(defn handle-cancellations! []
  (let [cancelled-job-set (into #{} (:cancel-job-ids @configs/configs))]
    (->> @configs/id->active-jobs
         (map (fn [[id {:keys [invalidate-at] :as job-meta}]]
                [id (assoc job-meta :cancel-source (cond (cancelled-job-set id) :manual-request
                                                         (t/after? (t/now) invalidate-at) :auto-expired))]))
         (filter (comp :cancel-source second))
         (map cancel-job!)
         dorun)))

(defn update-num-consumers! []
  (when @configs/consumers-enabled?
    (let [current-consumers (count @configs/consumer-ids)
          expected-consumers (:num-consumers @configs/configs)]
      ;add workers if there are a shortage
      (doseq [i (range current-consumers expected-consumers)]
        (future (binding [configs/*consumer-id* (UUID/randomUUID)]
                  (c/start-consumer!))))
      ;unlist extra consumers, let them close naturally
      (reset! configs/consumer-ids (take expected-consumers @configs/consumer-ids)))))

(defn supervise! []
  (loop [cntr 0]
    (try
      (handle-cancellations!)
      (update-num-consumers!)
      (catch Exception ex
        (timbre/error "An error occurred while supervising consumers:")
        (timbre/error ex)))
    (when (zero? cntr) (timbre/info (str "The following jobs are being supervised: " (mapv str (keys @configs/id->active-jobs)))))
    (Thread/sleep sleep-time)
    (recur (mod (inc cntr) 12))))