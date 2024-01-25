(ns mongo-jobq-ws.routes.services
  (:require
   [monger.collection :as mc]
   [ring.util.http-response :refer [ok]] ;[ok context]]
   [schema.core :as s]
   [mongo-jobq.configs :as sjc]
   [mongo-jobq.supervisor :as supervisor]
   [taoensso.timbre :as timbre]
   [util.app-status :as uap]
   [compojure.api.sweet :refer [GET POST api context]]
   [util.metrics :as metrics]
   [util.platform :as platform]))

(def service-routes
  (api
   {:swagger {:ui   "/swagger-ui"
              :spec "/swagger.json"
              :data {:info {:version     (platform/project-version "mongo-jobq")
                            :title       "MONGO JOBS API"
                            :description "Mongo Jobs Services"}}}}

   (context "/jobs" []
     {:tags ["jobs"]}

     (GET "/machine-stats" []
       :return {s/Any s/Any}
       :summary "Dump some raw machine metrics."
       (ok (metrics/get-raw-machine-stats)))

     (GET "/uptime" []
       :return Double
       :summary "How long has this machine been up and running, in minutes."
       (ok (/ (- (System/currentTimeMillis) metrics/startup-time) 1000 60.0)))

     (GET "/status" []
       :summary "Determine the worker's status"
       (ok (uap/status-components->status
            {}
            uap/file-system-status-component
            uap/mongo-status-component))))

    ;authenticated routes
   (context "/jobs" []
     (GET "/configs" []
       :return {s/Any s/Any}
       :summary "Dump the current configs atom."
       (ok @sjc/configs))

     (GET "/active-jobs" []
       :return [{s/Any s/Any}]
       :summary "Dump the current id->active-jobs atom."
       (ok (let [m @sjc/id->active-jobs]
             (mapv (fn [[k v]]
                     (let [job-doc (mc/find-one-as-map (sjc/db) "mongo_jobs" {:_id k})]
                       (into
                        (zipmap (keys v) (map str (vals v)))
                        {:_id  (str k)
                         :note "This can be found in mongo_jobq_configs.queue (or job_history at a future date)"}))) m))))

     (POST "/cancel-active-job" []
       :query-params [job-id :- s/Str]
       :summary "Cancel job with provided job-id, if it is running on this worker"
       (do (timbre/info (str "Received command to cancel job running on this worker with id: " job-id))
           (supervisor/manual-cancel-job! job-id)))

     (POST "/cancel-all-active-jobs" []
       :summary "Cancel all jobs currently running on this worker"
       (let [active-jobs-snapshot @sjc/id->active-jobs]
         (timbre/info "Received command to cancel all jobs currently running on this worker")
         (doseq [[oid _] active-jobs-snapshot]
           (supervisor/manual-cancel-job! oid))))

     (POST "/shutdown-worker" []
       :summary "Immediately shutdown the worker without necessarily completing currently running jobs."
       (do (timbre/info "Received command to shutdown worker. Exiting now.")
           (System/exit 0))))))