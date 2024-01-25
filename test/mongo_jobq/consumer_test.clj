(ns mongo-jobq.consumer-test
  (:require
   [clj-time.core :as t]
   [clj-time.coerce :as tc]
   [clojure.test :refer :all]
   [monger.collection :as mc]
   [mongo-jobq.configs :as configs]
   [mongo-jobq.consumer :as consumer]
   [mongo-jobq.test-fixtures :refer [with-db with-config with-no-op]]
   [taoensso.timbre :as timbre]))

(timbre/set-level! :warn)
(use-fixtures :each with-db with-config with-no-op)

(def present-job {:name "present-job" :run-after (tc/to-date (t/minus (t/now) (t/minutes 1))) :job-def {:type "scrape"} :est-time 60})
(def future-job {:name "future-job" :run-after (tc/to-date (t/plus (t/now) (t/minutes 30))) :job-def {:type "scrape"}})

(deftest no-queries
  (mc/insert (configs/db) "queue" present-job)
  (is (= present-job (dissoc (consumer/get-next-job) :_id))))

(deftest unmatching-query
  (swap! configs/configs assoc :queries ["{\"job-def.type\": {\"$ne\": \"scrape\"}}"])
  (mc/insert (configs/db) "queue" present-job)
  (is (nil? (consumer/get-next-job))))

(deftest matching-query
  (swap! configs/configs assoc :queries ["{\"job-def.type\": {\"$exists\": true}}"])
  (mc/insert (configs/db) "queue" present-job)
  (is (= present-job (dissoc (consumer/get-next-job) :_id))))

(deftest run-after-in-future
  (mc/insert (configs/db) "queue" future-job)
  (is (nil? (consumer/get-next-job))))

(deftest priority-foreground
  (let [now (tc/to-date (t/minus (t/now) (t/minutes 1)))
        bg-job {:name "bg-job" :priority "background" :run-after now :job-def {:type "no-op"}}
        fg-job {:name "fg-job" :priority "foreground" :run-after now :job-def {:type "no-op"}}]
    (mc/insert-batch (configs/db) "queue" [bg-job fg-job])
    (is (= fg-job (dissoc (consumer/get-next-job) :_id)))))

(deftest priority-normal
  (let [now (tc/to-date (t/minus (t/now) (t/minutes 1)))
        bg-job {:name "bg-job" :priority "background" :run-after now :job-def {:type "no-op"}}
        normal-job {:name "normal-job"  :run-after now :job-def {:type "no-op"}}]
    (mc/insert-batch (configs/db) "queue" [bg-job normal-job])
    (is (= normal-job (dissoc (consumer/get-next-job) :_id)))))

(deftest query-and-priority-order
  (swap! configs/configs assoc :queries ["{\"name\": \"bg-job\"}" "{\"name\": \"fg-job\"}"])
  (let [now (tc/to-date (t/minus (t/now) (t/minutes 1)))
        bg-job {:name "bg-job" :priority "background" :run-after now :job-def {:type "no-op"}}
        fg-job {:name "fg-job" :priority "foreground" :run-after now :job-def {:type "no-op"}}]
    (mc/insert-batch (configs/db) "queue" [bg-job fg-job])
    (is (= bg-job (dissoc (consumer/get-next-job) :_id)))))
