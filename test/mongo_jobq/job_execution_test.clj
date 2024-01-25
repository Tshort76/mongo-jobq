(ns mongo-jobq.job-execution-test
  (:require
   [clj-time.core :as t]
   [clj-time.coerce :as tc]
   [clojure.spec.alpha :as spec]
   [clojure.test :refer :all]
   [mongo-jobq.datasources :as ds]
   [monger.collection :as mc]
   [jobs.specs.job :as specs]
   [mongo-jobq.configs :as configs]
   [mongo-jobq.job-execution :as job-exec]
   [mongo-jobq.test-fixtures :refer [with-db with-config with-no-op]]
   [taoensso.timbre :as timbre])
  (:import (java.util UUID)))

(timbre/set-level! :warn)
(use-fixtures :each with-db with-config with-no-op)

(def test-job {:_id "test-id" :run-after (tc/to-date (t/now)) :name "no-op" :job-def {:type "no-op"}})

(deftest run-job
  (binding [configs/*consumer-id* (UUID/randomUUID)]
    (job-exec/safely-run-job test-job))
  (let [queue-job (first (mc/find-maps (configs/db) "queue"))
        history-job (first (mc/find-maps (configs/db) "job_history"))]
    (is (nil? queue-job)
        "completed job is removed from queue")
    (is (= "no-op" (:output history-job))
        "completed job is moved to job_history")
    (is (spec/valid? ::specs/historical-job-doc history-job)
        (spec/explain-str ::specs/historical-job-doc history-job))))
