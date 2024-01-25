(ns jobs.specs.scraper
  (:require [clojure.spec.alpha :as s])
  (:import (java.util.regex Pattern)))

(s/def ::index?               boolean?)
(s/def ::params->request      keyword?)
(s/def ::request->uploadable  keyword?)
(s/def ::published-on         (s/cat :type string? :args (s/* number?)))
(s/def ::tods                 (s/+ (s/and string? (partial re-matches #"\d?\d:\d\d"))))
(s/def ::est-time             (s/and integer? pos?))
(s/def ::file-exclusion-regex (partial instance? Pattern))

(s/def :retry/type            string?)
(s/def :retry/max-attempts    (s/and integer? pos?))
(s/def :retry/only-biz-days?  boolean?)
(s/def ::retry-specs          (s/keys :req-un [:retry/type :retry/max-attempts]
                                      :opt-un [:retry/only-biz-days?]))

(s/def :valid/pre-upload      keyword?)
(s/def :valid/post-upload     keyword?)
(s/def ::validation           (s/keys :opt-un [:valid/pre-upload :valid/post-upload]))

(s/def ::scraper (s/keys :opt-un [::index? ::request->uploadable ::params->request ::published-on
                                  ::tods ::retry-specs ::est-time
                                  ::file-exclusion-regex ::validation]))
