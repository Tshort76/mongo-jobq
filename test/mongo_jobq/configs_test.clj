(ns mongo-jobq.configs-test
  (:require
   [clojure.test :refer :all]
   [monger.collection :as mc]
   [mongo-jobq.configs :as configs]
   [mongo-jobq.test-fixtures :refer [with-db]]
   [taoensso.timbre :as timbre]))

(defn with-config [f]
  (with-redefs [configs/host "localhost"]
    (mc/insert (configs/db) "mongo_jobs" {:_id "localhost" :action "run"})
    (reset! configs/configs {})
    (f)))

(timbre/set-level! :warn)
(use-fixtures :each with-db with-config)

(deftest config-default
  (mc/remove (configs/db) "mongo_jobs")
  (configs/init-configs!)
  (is (= "pause" (:action @configs/configs))))

(deftest config-init
  (configs/init-configs!)
  (is (= "run" (:action @configs/configs))))
