(ns mongo-jobq.job-execution
  (:require [analytics.metrics :as metrics]
            [clojure.string :as cs]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [monger.collection :as mc]
            [pipeline.job-execution :as jex]
            [mongo-jobq.server-core :as ssc]
            [clojure.pprint :refer [pprint]]
            [mongo-jobq.configs :as configs]
            [pipeline.jobs.utils :as pju]
            [taoensso.timbre :as timbre])
  (:import (com.mongodb DuplicateKeyException)
           (java.util Date)
           (java.util.concurrent CancellationException)))

(defn format-err [error]
  (if (instance? Exception error)
    (some->> (with-out-str (pprint error))
             cs/split-lines
             (filter #(not (or
                            (cs/includes? % "\tat clojure.")
                            (cs/includes? % "[clojure."))))
             (#(if (> (count %) 100) (concat (take 80 %) (take-last 20 %)) %)))
    error))

(defn error->code [error]
  (Integer/toHexString (.hashCode (with-out-str (pprint error)))))

(def no-job-def-failure {:error ["AAAAA!!! Job(s) completed without a job-def."]
                         :host configs/host
                         :approx-time (Date.)
                         :note (str "There will only be one entry in unresolved_failures for this error, regardless of number of instances. "
                                    "This error will not be automatically cleared upon resolving the bug. It must be removed from unresolved_failures manually. "
                                    "Meanwhile, failures may slip through the cracks and not show up in unresolved_failures or job_history correctly. "
                                    "Strongly consider pausing all workers in any affected environment until mitigated/resolved.")})

(defn job->failure-fingerprint
  "Flattens {:job-def {:type 'scrape', :params {}}, ...} to be: {:job-def.type 'scrape', :job-def.params {}} w/o reschedule? flag"
  [job]
  (dissoc (reduce (fn [m [k v]] (assoc m (keyword (str "job-def." (name k))) v))
                  {} (:job-def job))
          :job-def.reschedule?))

(defn upsert-failure-with-new-id [job-fingerprint job]
  (when (try (ssc/do-with-db-retries (partial mc/insert-and-return (configs/db) "unresolved_failures" job))
             (catch DuplicateKeyException e
               (timbre/warn (str "Unable to upsert job into unresolved_failures because an unresolved_failure with id: "
                                 (:_id job) " already exists.")) true))
    (ssc/do-with-db-retries (partial mc/remove (configs/db) "unresolved_failures"
                                     (merge job-fingerprint {:completed-at {"$lt" (:completed-at job)}})))))

(defn update-unresolved-failures [{:keys [_id error cancellation-reason job-def] :as job}]
  (let [job-fingerprint (job->failure-fingerprint job)
        update-fn (if (or error cancellation-reason)
                    (do (timbre/info (str "Upserting job to unresolved_failures: " job-fingerprint))
                        (partial upsert-failure-with-new-id job-fingerprint job))
                    (do (timbre/info (str "Removing job from unresolved_failures: " job-fingerprint))
                        (partial mc/remove (configs/db) "unresolved_failures" job-fingerprint)))]
    (if job-def
      (ssc/do-with-db-retries update-fn)
      (ssc/do-with-db-retries (partial upsert-failure-with-new-id (configs/db) "unresolved_failures"
                                       (select-keys no-job-def-failure [:error]) no-job-def-failure)))))

(defn move-job-to-history [{:keys [_id] :as completed-job}]
  (let [insert-fn (partial mc/insert-and-return (configs/db) "job_history" completed-job)
        remove-fn (partial mc/remove-by-id (configs/db) "queue" _id)]
    (when (try (ssc/do-with-db-retries insert-fn)
               (catch DuplicateKeyException e
                 (timbre/warn (str "Completed job successfully, but found job with matching id in job_history. "
                                   "Removing job with id: " _id " from queue.")) true))
      (ssc/do-with-db-retries remove-fn))))

(defn complete-job [{:keys [_id name result cancellation-reason executions] :as job} start-time & {:keys [error unrecoverable]}]
  (try
    (let [end-time (Date.)
          run-time (if start-time (- (.getTime end-time) (.getTime start-time)) 0)
          execution-num (if (seq executions)
                          (-> executions count dec)
                          0)
          completed-job (cond-> job
                          true (dissoc :run-after)
                          true (assoc :completed-host configs/host :completed-worker configs/*consumer-id*
                                      :completed-at end-time :run-time run-time)
                          (empty? executions) (assoc :executions [{}]) ;; Bad Things happen when executions doesn't exist
                          true (update-in [:executions execution-num]
                                          assoc :end-time end-time :completion-state "success" :_id _id)
                          error (assoc :error (format-err error))
                          error (assoc :error-code (error->code error))
                          error (assoc :error-message (.getMessage error))
                          error (update-in [:executions execution-num]
                                           assoc :completion-state "error")
                          unrecoverable (assoc :unrecoverable true)
                          unrecoverable (update-in [:executions execution-num]
                                                   assoc :completion-state "unrecoverable")
                          cancellation-reason (assoc :cancellation-reason cancellation-reason)
                          cancellation-reason (update-in [:executions execution-num]
                                                         assoc :completion-state "cancelled"))]
      (metrics/record-job-complete completed-job)
      (update-unresolved-failures completed-job)
      (move-job-to-history completed-job)
      (timbre/info (str "job " name " finished " (if (or error cancellation-reason) "UNSUCCESSFULLY" "successfully") "\n")))
    (catch Throwable ex
      (let [cause-chain (take-while identity (iterate #(.getCause %) ex))]
        (if (some (partial instance? Error) cause-chain)
          (do (timbre/error (str "Failed to complete job with id: " _id " due to unrecoverable error:"))
              (timbre/error ex)
              (throw ex))
          (do (timbre/warn (str "Failed to complete job with id: " _id " due to exception:"))
              (timbre/warn ex)))))))

(defn update-job-on-start [{:keys [_id est-time start-times executions bid] :as job} start-time]
  (let [updated-fields {:start-times (conj (or start-times []) {:time start-time :host configs/host :worker configs/*consumer-id*})
                        :executions  (conj (or executions [])
                                           {:host configs/host :worker configs/*consumer-id* :start-time start-time :run-bid bid
                                            :trace "abcd"})
                        :run-after   (tc/to-date (-> (or est-time (* 2 60 60)) t/seconds t/from-now))
                        :trace       (re-find #".*(?=\.)" "abcd")}] ;leave off last segment (retry-num), as it is specific to this execution of the job
    (mc/update-by-id (configs/db) "queue" _id {"$set" updated-fields})
    (merge job updated-fields)))

(defn unsafe-exec [fnc arg]
  (when-let [result (fnc arg)]
    {:result   (:stored result result)
     :enqueued (:enqueued result :no-additional-jobs-queued)}))

(defn unsafe-execute [{:keys [job-def] :as job-doc}]
  (let [dispatch-key (-> job-def :type keyword)]
    (timbre/debug (str "Executing job with dispatch key " dispatch-key "."))
    (if-let [job-fn (pju/job-spec->func (get jex/job-type->spec dispatch-key))]
      (unsafe-exec job-fn job-doc)
      (throw (UnsupportedOperationException. "Unknown job type")))))

(defn execute-job [{:keys [_id name start-times] :as job} start-time]
  (timbre/info (str "Starting job: " _id " (" name ")"))
  (if (> (or (count start-times) 0) 3) ;if this job has been retried too many times, cancel it
    (assoc job :cancellation-reason :too-many-retries)
    (let [updated-job (update-job-on-start job start-time)
          {:keys [result]} (ssc/do-with-db-retries (partial unsafe-execute updated-job))]
      (assoc updated-job :output result))))

(defn run-registered-job [{:keys [_id est-time] :as job} start-time]
  (let [processor (future (execute-job job start-time))]
    (swap! configs/id->active-jobs assoc _id
           ;invalidate 10 minutes before expiration, so we can update the job doc
           {:future processor :consumer-id configs/*consumer-id*
            :invalidate-at (-> (or est-time (:default-est-time @configs/configs)) (- 600) t/seconds t/from-now)})
    @processor))

(defn handle-exception [{:keys [_id] :as job} start-time ex]
  (timbre/warn (str "Error on worker: " configs/*consumer-id* " when processing job with id: " _id ". Error: "))
  (timbre/warn ex)
  (complete-job job start-time :error ex))

(defn handle-unrecoverable-error [{:keys [_id] :as job} start-time error]
  (reset! configs/consumers-enabled? false) ;signal to consumer threads to stop consuming, and for supervisor to stop adding new consumer threads
  (timbre/error (str "UNRECOVERABLE ERROR on worker: " configs/*consumer-id* " when running a job with id: " _id ". Attempting to complete job. Error:"))
  (timbre/error error)
  (complete-job job start-time :error error :unrecoverable true) ;attempt to move job to history if possible
  (timbre/error "Marked job unrecoverable and moved it to job_history. Shutting down now...")
  ;exit to guarantee system shuts down - https://stuartsierra.com/2015/05/27/clojure-uncaught-exceptions
  (System/exit -1))

(defn safely-run-job [{:keys [_id] :as job}]
  (let [timer-context (metrics/record-job-start job)
        start-time (Date.)]
    (try (-> job
             (run-registered-job start-time)
             (complete-job start-time))
         ;TODO: Figure out how to include runtime in all cancelled jobs
         (catch InterruptedException _ (timbre/debug (str "Interrupted when running job: " _id)))
         (catch CancellationException _ (timbre/info "Job was canceled with id: " _id))
         (catch Exception ex
           (let [cause-chain (take-while identity (iterate #(.getCause %) ex))]
             (if (some (partial instance? Error) cause-chain)
               (handle-unrecoverable-error job start-time ex) ;if root cause is an error, consider it unrecoverable
               (handle-exception job start-time ex))))
         (catch Throwable t (handle-unrecoverable-error job start-time t))
         (finally (swap! configs/id->active-jobs dissoc _id)
                  (metrics/record-job-stop timer-context)))))
