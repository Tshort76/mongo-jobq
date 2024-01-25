(ns mongo-jobq.server-core
  (:require
   [mongo-jobq.datasources :as ds]
   [taoensso.timbre :as timbre])
  (:import (com.mongodb MongoTimeoutException MongoWriteException)))

(defn do-with-db-retries
  "Executes a function (which presumably interacts with Mongo) and will catch exceptions caused by a lost Mongo
   connection, and will retry the function once connection with mongo is re-established. First retry is immediate,
   but it will back off with each retry to avoid hammering Mongo as it is coming back online."
  [f]
  (loop [retry-count 0]
    (if (ds/mongo-working?)
      (when (pos? retry-count)
        (timbre/info "Connection with Mongo reestablished."))
      (let [sleep-seconds (-> retry-count (Math/pow 2) long (min 30))] ;retry intervals: [0 1 4 9 16 25 30 30 30 30...]
        (timbre/warn "Lost connection to Mongo. Trying again in " sleep-seconds " seconds.")
        (Thread/sleep (* sleep-seconds 1000))
        (recur (inc retry-count)))))
  (try (f)
       (catch MongoWriteException e (do-with-db-retries f))
       (catch MongoTimeoutException e (do-with-db-retries f))))
