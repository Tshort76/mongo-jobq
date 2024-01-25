(ns jobs.utils
  (:require [mongo-jobq.datasources :as ds]
            [monger.collection :as mc]
            [monger.conversion :as mcv]
            [monger.operators :refer [$exists $lt $gte $lte]]
            [monger.query :as mq]
            [monger.result :as mr]
            [schema.core :as s]
            [clojure.string :as cs]
            [clj-time.core :as t]
            [clj-time.format :as ctf]
            [clj-time.coerce :as tc])
  (:import (java.text SimpleDateFormat)
           (java.util UUID Date)
           (org.bson.types ObjectId)))

(def ^:dynamic *within-job-execution* false)

(def Job-def
  {(s/optional-key :type)        s/Str
   (s/optional-key :num-retries) s/Num
   s/Keyword                     s/Any})

(def Job
  {:job-def                   Job-def
   :run-after                 s/Inst
   :bid                       s/Str
   (s/optional-key :est-time) s/Num
   (s/optional-key :name)     s/Str
   s/Keyword                  s/Any})

(def priority-lvls '("foreground" nil "background"))

(defn update-id-str [x]
  (update x :_id (fn [o] (.toHexString o))))

;;;;;;;;;;;;;;;;;;;;;Queries and other useful information-gathering functions;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn jobs-summary
  "return the number of jobs that are currently running and the number in the queue" []
  (->> (mc/find-maps (ds/get-db "mongo_jobq_configs") "queue" {:output {$exists false} :error {$exists false}})
       (group-by #(contains? % :start-times))
       (map (fn [[_key _vals]] (if _key {:running (count _vals)} {:enqueued (count _vals)})))
       (apply merge)
       (#(if (empty? %) {:running 0 :enqueued 0} %))))

(defn count-active []
  (mc/count (ds/get-db "mongo_jobq_configs") "queue" {}))

(defn enqueued []
  (map update-id-str
       (mq/with-collection
         (ds/get-db "mongo_jobq_configs") "queue"
         (mq/find {:run-after {$lt (new Date)} :start-times {$exists false}})
         (mq/limit 100))))

(defn running []
  (->> {:run-after {$exists true} :start-times {$exists true}}
       (mc/find-maps (ds/get-db "mongo_jobq_configs") "queue")
       (map update-id-str)
       (sort-by #(some-> % :start-times first :time) #(compare %2 %1))))

(defn parse-date-or-default
  "parses dates in form mm/dd/yyyy or defaults to yesterday"
  [date-string]
  (let [today       (ctf/unparse (:year-month-day ctf/formatters) (t/minus (t/now) (t/days 1)))
        date-string (if (string? date-string)
                      (cs/replace date-string #"\/" "-")
                      "no date")
        date        (try
                      (ctf/parse (:year-month-day ctf/formatters) date-string)
                      (catch Exception e (ctf/parse (:year-month-day ctf/formatters) today)))]
    (tc/to-date date)))

(defn errors
  ([]
   (errors "" 100))
  ([as-of-date]
   (errors as-of-date 100))
  ([as-of-date limit]
   (let [date (parse-date-or-default as-of-date)]
     (map update-id-str
          (mq/with-collection
            (ds/get-db "mongo_jobq_configs") "job_history"
            (mq/find {:error {$exists true} :completed-at {"$gte" date}})
            (mq/limit limit))))))

(defn error-count
  [as-of-date]
  (let [date (parse-date-or-default as-of-date)]
    (mc/count
     (ds/get-db "mongo_jobq_configs")
     "job_history"
     {:error {"$exists" true} :completed-at {"$gte" date}})))

(defn warning-count
  [as-of-date]
  (let [date (parse-date-or-default as-of-date)]
    (mc/count
     (ds/get-db "mongo_jobq_configs")
     "job_history"
     {:output [] :completed-at {"$gte" date}})))

(defn recent-completes
  ([as-of-date]
   (recent-completes as-of-date 100))
  ([as-of-date limit]
   (let [date (.parse (SimpleDateFormat. "YYYY-MM-dd HH:mm:ss") as-of-date)]
     (map update-id-str
          (mq/with-collection
            (ds/get-db "mongo_jobq_configs") "job_history"
            (mq/find {:completed-at {$gte date} :error {$exists false}})
            (mq/fields [:completed-at :_id :job-def :bid :name])
            (mq/sort {:completed-at -1})
            (mq/limit limit))))))

(defn job-by-id [id]
  (or (mc/find-one-as-map (ds/get-db "mongo_jobq_configs") "queue" {:_id (ObjectId. ^String id)})
      (mc/find-one-as-map (ds/get-db "mongo_jobq_configs") "job_history" {:_id (ObjectId. ^String id)})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn time->objectid [time]
  (-> time .getTime (quot 1000) Integer/toHexString (str "0000000000000000") mcv/to-object-id))

(defn ids->range [ids]
  (let [ids (filter identity ids)]
    (if (not-empty ids)
      (reduce (fn [{:keys [^ObjectId min ^ObjectId max]} ^ObjectId obj-id]
                (hash-map :min (if (pos? (.compareTo min obj-id)) obj-id min)
                          :max (if (neg? (.compareTo max obj-id)) obj-id max)))
              {:min (ObjectId. "ffffffffffffffffffffffff")
               :max (ObjectId. "000000000000000000000000")} (flatten ids)))))

(defn id-range->ids [min-id max-id]
  (map
   #(ObjectId. (str (subs (str min-id) 0 18) (format "%06x" %)))
   (range (Integer/parseInt (subs (str min-id) 18) 16)
          (inc (Integer/parseInt (subs (str max-id) 18) 16)))))

(defn consec-ids->summary [obj-ids]
  (let [{:keys [min max]} (ids->range obj-ids)]
    {:count  (->> obj-ids flatten (filter identity) count)
     :min-id min
     :max-id max}))

(defn decorate-job-doc [{:keys [bid stime] :as jdoc}]
  (let [bid   (or bid (str (UUID/randomUUID)))
        stime (or stime (new Date))
        trace "abcd"
        jdoc  (select-keys jdoc [:job-def :name :priority :est-time :retry-num :rerun-bid])]
    (when (seq jdoc)
      (s/validate Job (assoc jdoc :run-after stime :bid bid :trace trace)))))

(defn enqueue-job [job-doc]
  (when-let [{:keys [bid] :as job} (some-> (decorate-job-doc job-doc) (assoc :_id (new ObjectId)))]
    (if (mr/acknowledged? (mc/insert (ds/get-db "mongo_jobq_configs") "queue" job))
      {:bid     bid
       :job-doc job
       :msg     (str "Job was added to the queue. Track its progress with bid : " bid)}
      {:msg "Error: job was NOT added to work queue."})))

(defn enqueue-jobs! [job-docs]
  (when (seq job-docs)
    (->> job-docs
         (keep decorate-job-doc)
         (ds/bulk-write (ds/get-db "mongo_jobq_configs") "queue")
         consec-ids->summary
         (#(assoc % :db "mongo_jobq_configs" :collection "queue")))))

(defn rerun-job [id-hex & [priority]]
  (if-let [job-doc (-> "mongo_jobq_configs"
                       ds/get-db
                       (mc/find-one-as-map "job_history" {:_id (ObjectId. id-hex)}))]
    (do (enqueue-job (assoc (select-keys job-doc [:job-def :bid :name :est-time :executions :trace]) :priority priority))
        (select-keys job-doc [:bid :name]))
    (str "Could not find a job with _id = " id-hex)))

(defn remove-old-pre-norm [jobs]
  (->> jobs
       (group-by #(get-in % [:job-def :md5]))
       (mapv (comp #(select-keys % [:bid :name :_id]) last #(sort-by :_id %) second))))

(defn find-parse-jobs-by-md5 [md5s]
  (remove-old-pre-norm
   (mc/find-maps (ds/get-db "mongo_jobq_configs") "job_history"
                 {:job-def.type {"$in" ["parse-file" "parse-surveyor-answer"]}
                  :job-def.md5  {"$in" md5s}}
                 {:bid 1 :job-def 1 :name 1})))

(defn renormalize-jobs [parse-jobs error-msg]
  (let [pjob-bid->name (->> parse-jobs
                            (map #(hash-map (:bid %) (:name %)))
                            (reduce merge))]
    (if (seq pjob-bid->name)
      (->> pjob-bid->name
           keys
           (partition-all 200)
           (mapcat #(mc/find-maps (ds/get-db "mongo_jobq_configs") "job_history"
                                  {:job-def.type "normalize"
                                   :bid          {"$in" %}}
                                  {:_id 1 :bid 1 :job-def 1}))
           (group-by #(get-in % [:job-def :min-id]))
           (map (comp (fn [{id :_id}] (-> id .toHexString (rerun-job "background"))) first second))
           (group-by :bid)
           (map #(hash-map :bid (first %) :job-count (count (second %)) :name (get pjob-bid->name (first %))))
           doall)
      error-msg)))

(defn renormalize [file-type]
  (renormalize-jobs
   (find-parse-jobs-by-file-type file-type)
   (str "No documents found for file-type: " file-type)))

(defn renormalize-md5s [md5s]
  (renormalize-jobs
   (find-parse-jobs-by-md5 md5s)
   (str "No documents found for md5s " md5s)))

(defn inspect-renormalize [file-type]
  (if-let [pjob-bid->name (->> file-type
                               find-parse-jobs-by-file-type
                               (map #(hash-map (:bid %) (:name %)))
                               (reduce merge))]
    (->> pjob-bid->name
         keys
         (partition-all 200)
         (mapcat #(mc/find-maps (ds/get-db "mongo_jobq_configs") "job_history" {:job-def.type "normalize" :bid {"$in" %}}
                                {:_id 1 :bid 1 :job-def 1}))
         (group-by :job-def)
         (#(hash-map :num-files (count (keys pjob-bid->name))
                     :num-normalization-jobs (count %)
                     :files (->> pjob-bid->name
                                 vals
                                 (map (comp cs/trim second (fn [x] (cs/split x #":"))))))))
    {:num-files 0 :num-normalization-jobs 0 :files []}))

(defn id-between [min-id-hex max-id-hex]
  {:_id {$gte (mcv/to-object-id min-id-hex) $lte (mcv/to-object-id max-id-hex)}})

(defn docs-in-range [db-name coll-name min-obj-hex max-obj-hex]
  (mc/find-maps (ds/get-db db-name) coll-name (id-between min-obj-hex max-obj-hex)))

(defn docs-in-set [db-name coll-name ids]
  (mc/find-maps (ds/get-db db-name) coll-name {:_id {"$in" (mapv mcv/to-object-id ids)}}))

(defn docs-in-batch [db-name coll-name min-obj-hex max-obj-hex]
  (map (comp #(dissoc % :meta) update-id-str)
       (docs-in-range db-name coll-name min-obj-hex max-obj-hex)))
