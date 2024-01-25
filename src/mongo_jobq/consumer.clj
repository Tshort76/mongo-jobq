(ns mongo-jobq.consumer
  (:require [analytics.metrics :as metrics]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [mongo-jobq.datasources :as ds]
            [monger.operators :refer :all]
            [mongo-jobq.server-core :as ssc]
            [mongo-jobq.configs :as configs]
            [mongo-jobq.job-execution :as exec]
            [jobs.utils :as sju]
            [taoensso.timbre :as timbre]
            [clojure.data.json :as json])
  (:import (java.util Date)))

; Shamelessly copied from monger: https://github.com/michaelklishin/monger/blob/master/src/clojure/monger/collection.clj#L196
(defn execute-job-query [query]
  (ssc/do-with-db-retries
   (partial ds/find-and-modify
            (ds/get-db "mongo_jobq_configs") "queue"
            (merge {:run-after {"$lt" (new Date)}} query)
            {"$set" {:run-after (-> @configs/configs :default-est-time t/seconds t/from-now tc/to-date)}}
            {})))

(defn query->priorities [query]
  (if-let [query-priority (:priority query)] [query-priority] sju/priority-lvls))

(defn get-next-job []
  (try (let [decoded-queries (mapv json/read-str (:queries @configs/configs))
             query-priority-combinations (for [query-index (range (count decoded-queries))
                                               priority (query->priorities (nth decoded-queries query-index))]
                                           {:query-index query-index :priority priority})]
         (some (fn [{:keys [query-index priority]}]
                 (let [query (assoc (nth decoded-queries query-index) :priority (or priority {"$ne" "background"}))
                       job (execute-job-query query)]
                   (when (not-empty job)
                     (metrics/record-job-polled job query-index priority)
                     job)))
               query-priority-combinations))
       (catch Exception e (timbre/error "Exception occurred while searching for a job.")
              (timbre/error e))
       (catch Error e (timbre/error "Unrecoverable error while polling. Shutting down now...")
              (timbre/error e)
              (System/exit -1))))

(defn poll-and-run [cnt]
  (let [{:keys [action pause-wait empty-Q-wait]} @configs/configs]
    (if (= action "pause")
      (do (timbre/debug (str "Pause action acknowledged. Consumer " configs/*consumer-id* " will sleep for "
                             pause-wait " seconds before checking for a status update."))
          (Thread/sleep (* 1000 pause-wait)))
      (if-let [{:keys [_id trace start-times executions] :as job-to-run} (get-next-job)]
        (exec/safely-run-job job-to-run)
        (do
          (when (zero? cnt) (timbre/trace (str "No jobs found by consumer " configs/*consumer-id* ", will continue to monitor every " empty-Q-wait " seconds.")))
          (Thread/sleep (* 1000 empty-Q-wait)))))))

(defn start-consumer! []
  (swap! configs/consumer-ids conj configs/*consumer-id*)
  (timbre/debug (str "Starting consumer " configs/*consumer-id* " on host: " configs/host))
  (loop [cnt 0]
    (when (and @configs/consumers-enabled? (some #(= configs/*consumer-id* %) @configs/consumer-ids))
      (poll-and-run cnt)
      (recur (mod (inc cnt) 10))))
  (timbre/info (str "Consumer " configs/*consumer-id* " was shut down due to config change.")))

