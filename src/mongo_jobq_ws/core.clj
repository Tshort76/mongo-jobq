(ns mongo-jobq-ws.core
  (:require [clojure.tools.nrepl.server :as nrepl]
            [environ.core :refer [env]]
            [immutant.web :as immutant]
            [mongo-jobq-ws.handler :refer [app destroy init]]
            [taoensso.timbre :as timbre]
            [mongo-jobq.core :as sjc]
            [mongo-jobq.datasources :as ds]
            [monger.collection :as mc]
            [mongo-jobq.manifest :as manifest])
  (:gen-class)
  (:import (java.util Date)
           (java.net InetAddress)))

(defonce nrepl-server (atom nil))

(defn parse-port [port]
  (when port
    (cond
      (string? port) (Integer/parseInt port)
      (number? port) port
      :else          (throw (Exception. (str "invalid port value: " port))))))

(defn stop-nrepl []
  (when-let [server @nrepl-server]
    (nrepl/stop-server server)))

(defn start-nrepl
  "Start a network repl for debugging when the :nrepl-port is set in the environment."
  []
  (if @nrepl-server
    (timbre/error "nREPL is already running!")
    (when-let [port (env :nrepl-port)]
      (try
        (->> port
             (parse-port)
             (nrepl/start-server :bind "0.0.0.0" :port)
             (reset! nrepl-server))
        (timbre/info "nREPL server started on port" port)
        (catch Throwable t
          (timbre/error t "failed to start nREPL"))))))

(defn http-port []
  (parse-port (or (env :port) 8080)))

(defonce http-server (atom nil))

(defn start-http-server [port]
  (reset! http-server (immutant/run app :host "0.0.0.0" :port port)))

(defn stop-http-server []
  (when @http-server
    (destroy)
    (immutant/stop @http-server)
    (reset! http-server nil)))

(defn stop-app []
  (stop-nrepl)
  (stop-http-server)
  (shutdown-agents))

(defn publish-startup []
  (mc/insert (ds/get-db "mongo_jobq_configs") "startups"
             {:start-time (Date.)
              :hostname   (.getHostName (InetAddress/getLocalHost))
              :manifest   (manifest/as-map)}))

(defn start-app []
  (.addShutdownHook (Runtime/getRuntime) (Thread. stop-app))
  (start-nrepl)
  (init)
  (start-http-server (http-port))
  (publish-startup)
  (timbre/info "server started on port:" (:port @http-server)))

(defn -main []
  (start-app)
  (sjc/launch)
  @(promise)) ;Needed to prevent immediate shutdown when started with 'lein run' from a Windows machine