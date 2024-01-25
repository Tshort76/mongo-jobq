(ns mongo-jobq-ws.handler
  (:require [analytics.metrics :as metrics]
            [compojure.core :refer [routes wrap-routes]]
            [compojure.route :as route]
            [environ.core :refer [env]]
            [selmer.parser :as parser]
            [mongo-jobq-ws.layout :refer [error-page]]
            [mongo-jobq-ws.middleware :as middleware]
            [mongo-jobq-ws.routes.home :refer [home-routes]]
            [mongo-jobq-ws.routes.services :refer [service-routes]]
            [taoensso.timbre :as timbre]))

(def uploader-state (atom {}))

(defn init
  "init will be called once when app is deployed as a servlet
   on an app server such as Tomcat put any initialization code here"
  []
  (when (env :dev) (parser/cache-off!))
  (timbre/info (str "\n-=[mongo_jobs_ws started successfully" (when (env :dev) " using the development profile") "]=-")))

(defn destroy
  "destroy will be called when your application shuts down, put any clean up code here"
  []
  (swap! uploader-state assoc :running false)
  (timbre/info "mongo_jobs_ws is shutting down...")
  (timbre/info "shutdown complete!"))

;http://blog.darevay.com/2010/11/compojure-the-repl-and-vars/
;This explains using vars/#' vs calling out the field directly.
(def app-routes
  (routes
   (wrap-routes #'service-routes metrics/wrap-api-metrics)
   (wrap-routes #'home-routes middleware/wrap-csrf)
   (route/not-found
    (:body
     (error-page {:status 404
                  :title "page not found"})))))

(def app (when-not *compile-files* (middleware/wrap-base #'app-routes)))