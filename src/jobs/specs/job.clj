(ns jobs.specs.job
  (:require [clojure.spec.alpha :as s]))

(s/def ::bid string?)
(s/def ::cancellation-reason (s/or :string string? :keyword keyword?))
(s/def ::completed-at inst?)
(s/def ::completed-host string?)
(s/def ::completed-worker (s/or :string string? :uuid uuid?))
(s/def ::error vector?)
(s/def ::error-code number?)
(s/def ::error-message string?)
(s/def ::est-time number?)
(s/def ::host (s/or :string string? :keyword keyword?))
(s/def ::job-def (s/keys :req-un [::type]))
(s/def ::name (s/or :string string? :keyword keyword?))
(s/def ::output (complement nil?))
(s/def ::priority (s/or :string string? :keyword keyword?))
(s/def ::run-after inst?)
(s/def ::run-time number?)
(s/def ::start-time (s/keys :req-un [::time ::host ::worker]))
(s/def ::start-times (s/coll-of ::start-time))
(s/def ::time inst?)
(s/def ::type (s/or :string string? :keyword keyword?))
(s/def ::unrecoverable boolean?)
(s/def ::worker (s/or :string string? :uuid uuid?))

(s/def ::job-doc
  (s/keys :req-un [::job-def ::name ::run-after]
          :opt-un [::bid ::est-time ::priority]))

(s/def ::historical-job-doc
  (s/keys :req-un [::completed-at
                   ::completed-host
                   ::completed-worker
                   ::job-def
                   ::name
                   ::run-time
                   ::start-times]
          :opt-un [::bid
                   ::cancellation-reason
                   ::error
                   ::error-code
                   ::error-message
                   ::est-time
                   ::output
                   ::priority
                   ::unrecoverable]))
