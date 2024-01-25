(ns mongo-jobq-ws.middleware
  (:require [environ.core :refer [env]]
            [immutant.web.middleware :refer [wrap-session]]
            [mongo-jobq-ws.layout :refer [*app-context* error-page]]
            [prone.middleware :refer [wrap-exceptions]]
            [ring.middleware.anti-forgery :refer [wrap-anti-forgery]]
            [ring.middleware.defaults :refer [site-defaults wrap-defaults]]
            [ring.middleware.flash :refer [wrap-flash]]
            [ring.middleware.format :refer [wrap-restful-format]]
            [ring.middleware.reload :as reload]
            [ring.middleware.webjars :refer [wrap-webjars]]
            [selmer.middleware :refer [wrap-error-page]]
            [taoensso.timbre :as timbre]
            [util.auth.middleware :as auth]
            [compojure.api.sweet :refer :all])
  (:import (javax.servlet ServletContext)))

(defn wrap-context [handler]
  (fn [request]
    (binding [*app-context*
              (if-let [context (:servlet-context request)]
                ;; If we're not inside a servlet environment
                ;; (for example when using mock requests), then
                ;; .getContextPath might not exist
                (try (.getContextPath ^ServletContext context)
                     (catch IllegalArgumentException _ context))
                ;; if the context is not specified in the request
                ;; we check if one has been specified in the environment
                ;; instead
                (:app-context env))]
      (handler request))))

(defn wrap-internal-error [handler]
  (fn [req]
    (try
      (handler req)
      (catch Throwable t
        (timbre/error t)
        (error-page {:status 500
                     :title "Something very bad has happened!"
                     :message "We've dispatched a team of highly trained gnomes to take care of the problem."})))))

(defn wrap-dev [handler]
  (if (env :dev)
    (-> handler
        reload/wrap-reload
        wrap-error-page
        wrap-exceptions)
    handler))

(defn wrap-csrf [handler]
  (wrap-anti-forgery
   handler
   {:error-response
    (error-page
     {:status 403
      :title "Invalid anti-forgery token"})}))

(defn wrap-formats [handler]
  (wrap-restful-format handler
                       {:formats
                        [:edn :json :json-kw :transit-json :transit-msgpack
                         :yaml :yaml-in-html :yaml-kw]}))

(defn wrap-base [handler]
  (-> handler
      auth/handle-authentication-middleware
      wrap-dev
      wrap-formats
      wrap-webjars
      wrap-flash
      (wrap-session {:cookie-attrs {:http-only true}})
      (wrap-defaults
       (-> site-defaults
           (assoc-in [:security :anti-forgery] false)
           (dissoc :session)))
      wrap-context
      wrap-internal-error))