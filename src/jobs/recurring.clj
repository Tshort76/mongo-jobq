(ns jobs.recurring
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [jobs.rescheduling-utils :as ru]
            [jobs.utils :as jutils]))

(defn ->run-time [{:keys [published-on tods]}]
  (->> (or tods ["10:00"])
       ru/min-tod
       (t/plus (ru/next-available-date published-on))))

(defn- add-estimated-time [doc t1 t2]
  (let [est (some->> [t1 t2] (filter identity) not-empty (apply max))]
    (cond-> doc
      est (assoc :est-time est))))

(defn schedule! [{:keys [est-time] :as schedule-specs}
                 {doc-est-time :est-time :as job-doc}]
  (if (ru/unique? job-doc)
    (-> job-doc
        (add-estimated-time est-time doc-est-time)
        (dissoc :bid)
        (assoc :stime (tc/to-date (->run-time schedule-specs))
               :retry-num 0)
        jutils/enqueue-job
        (assoc :status :success))
    {:status :duplicate}))

(defn reschedule!
  "Reschedules a job, depending on if the job's last executed
   was successful or not (failed?). doc-updater-fn is applied to
   the job-doc in order to update its parameters for the next iteration.

  Returns {:keys [status]}, where status is in
   #{:duplicate - job is not unique, was not rescheduled
     :error - All retry attempts failed and no results were produced
     :warning - The job did not produce results and will retry again
     :success - The job completed successfully}"
  [{tods :tods run-every-tod? :run-every-tod? published-on :published-on
    {:keys [max-attempts] :as retry-spec} :retry-specs :as specs}
   {retry-num :retry-num {:keys [reschedule?]} :job-def :as job-doc}
   failed?
   doc-updater-fn]
  (when published-on
    (let [schedule-next-round (fn [] (when-not (false? reschedule?) (schedule! specs (doc-updater-fn job-doc))))
          schedule-same-day (fn [status time] (-> (if (= :warning status) (update job-doc :retry-num #(if % (inc %) 1)) job-doc)
                                                  (assoc :stime (tc/to-date time))
                                                  jutils/enqueue-job
                                                  (assoc :status status)))]
      (if failed?
        (if (and max-attempts (>= (or retry-num 0) (* (max (count tods) 1) (or max-attempts 1))))
          (assoc (schedule-next-round) :status :error :msg "Exhausted all retry attempts without success")
          (if-let [nxt-time (or (ru/nxt-runtime tods)       ;run later today if tod remains, otherwise schedule for different day
                                (and retry-spec (ru/retry-at retry-spec (ru/min-tod (or tods ["10:00"])))))]
            (schedule-same-day :warning nxt-time)
            (schedule-next-round)))
        (if-let [nxt-time (and run-every-tod? (ru/nxt-runtime tods))]
          (schedule-same-day :success nxt-time)
          (schedule-next-round))))))