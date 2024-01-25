(ns mongo-jobq.test-fixtures
  (:require
   [mongo-jobq.datasources :as ds]
   [monger.collection :as mc]
   [pipeline.job-execution :as job-exec]
   [pipeline.jobs.utils :as pju]
   [mongo-jobq.configs :as configs]))

(defn with-db [f]
  (ds/with-fongo (doall (f))))

(defn with-config [f]
  (with-redefs [configs/host "localhost"]
    (mc/insert (configs/db) "mongo_jobs" {:_id "localhost" :action "run" :num-consumers 1 :pause-wait 0 :empty-Q-wait 0})
    (configs/refresh-configs!)
    (reset! configs/consumer-ids [])
    (reset! configs/id->active-jobs {})
    (f)))

(defn with-no-op [f]
  (let [no-op-job {:load-fn (constantly :no-op), :process-fn identity, :store-fn identity}
        jt->jf (assoc job-exec/job-type->spec :no-op {:func pju/job :spec no-op-job})]
    (with-redefs [job-exec/job-type->spec jt->jf]
      (f))))
