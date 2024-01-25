(ns jobs.unresolved-failures
  (:require
   [mongo-jobq.datasources :as ds]
   [monger.collection :as mc]
   [monger.query :as mq]
   [jobs.utils :as sju]
   [mongo-jobq.server-core :as ssc]
   [taoensso.timbre :as timbre]
   [clj-time.coerce :as tc]
   [clj-time.core :as t])
  (:import (java.util UUID)
           (org.bson.types ObjectId)))

(defn ->object-id [job-id]
  (cond (= (type job-id) ObjectId) job-id
        (string? job-id) (ObjectId. ^String job-id)
        :default (throw (IllegalArgumentException. "job-id must be a valid ObjectId or string representation of one."))))

; Category Queries

(def unresolved-failure-queries {:all {}
                                 :recoverable-errors {:error {"$exists" true} :unrecoverable {"$exists" false}}
                                 :unrecoverable-errors {:unrecoverable {"$exists" true}}
                                 :all-cancelled {:cancellation-reason {"$exists" true}}
                                 :cancelled-manual {:cancellation-reason :manual-request}
                                 :cancelled-exceeded-est-time {:cancellation-reason :auto-expired}
                                 :cancelled-too-many-retries {:cancellation-reason :too-many-retries}})

; Rerun Unresolved Failures

(defn rerun-unresolved-failures [query & {:keys [uuid priority] :or {uuid (UUID/randomUUID)}}]
  (let [batch-find (assoc query :rerun-uuids {"$ne" uuid})
        batch-query (fn [] (mq/with-collection (ds/get-db "mongo_jobq_configs") "unresolved_failures"
                             (mq/find batch-find)
                             (mq/limit 100)))
        update-fn (fn [doc] (mc/update-by-id (ds/get-db "mongo_jobq_configs") "unresolved_failures"
                                             (:_id doc) (assoc doc :rerun-uuids (conj (or (:rerun-uuids doc) []) uuid))))]
    (loop [batch-docs (ssc/do-with-db-retries batch-query) batch-num 1]
      (if (seq batch-docs)
        (do (timbre/debug (str "Unresolved_failure (rerun-bid=" uuid ") is starting batch: " batch-num " with " (count batch-docs) " jobs in the batch."))
            (doseq [doc batch-docs]
              (-> doc
                  (select-keys [:job-def :name :est-time :bid :executions :trace])
                  (assoc :rerun-bid (str uuid) :priority priority)
                  (update :job-def dissoc :reschedule?)
                  sju/enqueue-job)
              (ssc/do-with-db-retries (partial update-fn doc)))
            (recur (ssc/do-with-db-retries batch-query) (inc batch-num)))
        (timbre/debug (str "Completed queueing all unresolved_failures for re-run: " uuid))))
    {:rerun-bid uuid}))

(defn rerun-unresolved-failures-by-category [category & {:keys [priority]}]
  (let [uuid (UUID/randomUUID)
        query (get unresolved-failure-queries category)]
    (if query
      (do (timbre/debug "Beginning re-run of unresolved_failures in category: " category " with uuid: " uuid)
          (rerun-unresolved-failures query :uuid uuid :priority priority))
      (throw (IllegalArgumentException. "Invalid unresolved_failures category")))))

(defn rerun-unresolved-failures-with-failure [failure & {:keys [priority]}]
  (if failure
    (let [uuid (UUID/randomUUID)]
      (timbre/debug "Beginning re-run of unresolved_failures failure: " failure " with uuid: " uuid)
      (rerun-unresolved-failures {"$or" [{:error-code failure} {:cancellation-reason failure}]} :uuid uuid :priority priority))
    (throw (IllegalArgumentException. "Invalid error-code or cancellation-reason: nil"))))

(defn rerun-unresolved-failures-with-matching-failures [job-id & {:keys [priority]}]
  (let [normalized-id (->object-id job-id)
        doc (ssc/do-with-db-retries (partial mc/find-map-by-id (ds/get-db "mongo_jobq_configs") "unresolved_failures" normalized-id))]
    (rerun-unresolved-failures-with-failure (or (:error-code doc) (:cancellation-reason doc)) :priority priority)))

(defn rerun-unresolved-failure-by-id [job-id & {:keys [priority]}]
  (let [normalized-id (->object-id job-id)]
    (rerun-unresolved-failures {:_id normalized-id} :priority priority)))

; Inspect Unresolved Failures

(defn inspect-unresolved-failures [query & {:keys [limit]}]
  (let [query-fn (if limit
                   (fn [] (mq/with-collection (ds/get-db "mongo_jobq_configs") "unresolved_failures"
                            (mq/find query)
                            (mq/limit limit)))
                   (fn [] (mq/with-collection (ds/get-db "mongo_jobq_configs") "unresolved_failures"
                            (mq/find query))))]
    (ssc/do-with-db-retries query-fn)))

(defn inspect-unresolved-failures-by-category [category & {:keys [limit]}]
  (let [query (get unresolved-failure-queries category)]
    (if query
      (inspect-unresolved-failures query :limit limit)
      (throw (IllegalArgumentException. "Invalid unresolved_failures category")))))

(defn inspect-unresolved-failures-with-failure [failure & {:keys [limit]}]
  (if failure
    (inspect-unresolved-failures {"$or" [{:error-code failure} {:cancellation-reason failure}]} :limit limit)
    (throw (IllegalArgumentException. "Invalid error-code or cancellation-reason: nil"))))

(defn inspect-unresolved-failures-with-matching-failures [job-id & {:keys [limit]}]
  (let [normalized-id (->object-id job-id)
        doc (ssc/do-with-db-retries (partial mc/find-map-by-id (ds/get-db "mongo_jobq_configs") "unresolved_failures" normalized-id))]
    (inspect-unresolved-failures-with-failure (or (:error-code doc) (:cancellation-reason doc)) :limit limit)))

(defn inspect-unresolved-failure-by-id [job-id]
  (let [normalized-id (->object-id job-id)]
    (inspect-unresolved-failures {:_id normalized-id} :limit 1)))

; Count Unresolved Failures

(defn count-unresolved-failures [query]
  (ssc/do-with-db-retries (fn [] (mc/count (ds/get-db "mongo_jobq_configs") "unresolved_failures" query))))

(defn count-unresolved-failures-by-category [category]
  (let [query (get unresolved-failure-queries category)]
    (if query
      (count-unresolved-failures query)
      (throw (IllegalArgumentException. "Invalid unresolved_failures category")))))

(defn count-unresolved-failures-with-failure [failure]
  (if failure
    (count-unresolved-failures {"$or" [{:error-code failure} {:cancellation-reason failure}]})
    (throw (IllegalArgumentException. "Invalid error-code or cancellation-reason: nil"))))

(defn count-unresolved-failures-with-matching-failures [job-id]
  (let [normalized-id (->object-id job-id)
        doc (ssc/do-with-db-retries (partial mc/find-map-by-id (ds/get-db "mongo_jobq_configs") "unresolved_failures" normalized-id))]
    (count-unresolved-failures-with-failure (or (:error-code doc) (:cancellation-reason doc)))))

; Aggregate

(defn distinct-failures [& {:keys [look-limit return-limit since]}]
  (mc/aggregate (ds/get-db "mongo_jobq_configs") "unresolved_failures"
                [;look at no more than 'look-limit' failed documents (prevent slow aggregation)
                 {"$match" {:completed-at {"$gte" (or since (tc/to-date (t/epoch)))}}}
                 {"$limit" look-limit}
                 {"$group" {:_id {"$ifNull" ["$error-code" "$cancellation-reason"]}
                            :count {"$sum" 1}
                            :job-types {"$addToSet" "$job-def.type"}
                            :jobs {"$push" "$_id"}
                            :error {"$first" "$error"}
                            :error-message {"$first" {"$ifNull" ["$error-message" "$cancellation-reason"]}}}}
                 {"$sort" {:count -1}}
                 ;return no more than 'return-limit' distinct failures (prevent large payload)
                 {"$limit" return-limit}]
                :cursor 20))
                ; :allow-disk-use true))
