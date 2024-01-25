(ns jobs.rescheduling-utils
  (:require [clj-time.core :as t]
            [clj-time.format :as tf]
            [clj-time.predicates :as pr]
            [mongo-jobq.datasources :as dsc]
            [monger.collection :as mc]))

(defn quarter [date]
  (int (Math/ceil (/ (t/month date) 3))))

(defn dformat [date pattern]
  (tf/unparse (tf/formatter pattern) date))

(defn today-at-start-of-day []
  (t/with-time-at-start-of-day (t/now)))

(defn now-str []
  (tf/unparse (tf/formatters :basic-date) (t/now)))

(defn tomorrow [tod]
  (t/plus (today-at-start-of-day) (t/days 1) tod))

(defn in-an-hour []
  (t/plus (t/now) (t/hours 1)))

(defn nth-biz-day [^Long n date]
  (->> date
       t/first-day-of-the-month
       (iterate #(t/plus % (t/days 1)))
       (reduce (fn [cnt day]
                 (if (and (pr/weekday? day) (>= cnt n))
                   (reduced day)
                   (if (pr/weekday? day) (inc cnt) cnt))) 1)))

(defn nth-day-nxt-month [^Long n]
  (-> (today-at-start-of-day)
      t/first-day-of-the-month
      (t/plus (t/months 1))
      (#(let [num-days (dec (min n (t/number-of-days-in-the-month %)))]
          (t/plus % (t/days num-days))))))

(defn first-day-nxt-quarter
  ([] (first-day-nxt-quarter (t/now)))
  ([now] (let [curr-month (t/month now)
               first-month-this-quarter (-> (quarter now) dec (* 3) inc)]
           (-> (t/with-time-at-start-of-day now)
               t/first-day-of-the-month
               (t/plus (t/months (- 3 (- curr-month first-month-this-quarter))))))))

(defn first-day-this-quarter [now]
  (with-redefs [t/now (fn [] now)]
    (t/minus (first-day-nxt-quarter) (t/months 3))))

(defn round-to-biz-day [date fwd?]
  (->> date
       (iterate #(let [fnc (if fwd? t/plus t/minus)]
                   (fnc % (t/days 1))))
       (some #(if (pr/weekday? %) %))))

(defn nth-biz-day-from [n from prior?]
  (->> from
       nth-day-nxt-month
       (iterate #((if prior? t/minus t/plus) % (t/days 1)))
       rest
       (filter pr/weekday?)
       (take n)
       last))

(defn tod->temporal [tod]
  (let [[_ hours mins] (re-matches #"(\d{1,2}):(\d{2})" tod)]
    (t/minutes (+ (Integer. mins) (* 60 (Integer. hours))))))

;tod format  hh:mm  or hh
(defn tods->date-times [date & tods]
  (let [tods (or tods ["10:00"])]
    (map (comp
          (partial t/plus (t/with-time-at-start-of-day date))
          tod->temporal)
         tods)))

(defn min-tod [tods]
  (some->> tods
           (map (comp t/in-minutes tod->temporal))
           (apply min)
           t/minutes))

(defn nxt-runtime [tods]
  (let [now (t/now)]
    (some->> tods
             (apply tods->date-times (today-at-start-of-day))
             (filter #(t/before? now %))
             not-empty
             t/earliest)))

(defn biz-day? [date & [lag]]
  ((into #{} (map (partial + (or lag 0)) (range 1 6)))
   (t/day-of-week date)))

(defn next-biz-day [date & [lag]]
  (let [lag (or lag 0)]
    (->> date
         (iterate #(t/plus % (t/days 1)))
         rest
         (filter #(biz-day? % lag))
         first)))

(defn biz-day-params [now lag]
  (t/minus (next-biz-day now lag) (t/days lag)))

(def spec->frequency {:nth-biz-day :monthly
                      :nth-biz-day-prior-to :monthly
                      :nth-biz-day-after :monthly
                      :nth-day :monthly
                      :daily :daily
                      :biz-daily :biz-daily
                      :nth-week-day :weekly
                      :quarterly :quarterly})


;;;;;;  PUBLISHED ON DEFS   ;;;;;;;
(defmulti spec->date (comp keyword first))

(defmethod spec->date :nth-biz-day [[_ ^Long n]]
  (->> (t/months 1)
       (t/plus (t/first-day-of-the-month (today-at-start-of-day)))
       (nth-biz-day n)))

(defmethod spec->date :nth-biz-day-prior-to [[_ ^Long n ^Long prior-to]]
  (nth-biz-day-from n prior-to true))

(defmethod spec->date :nth-biz-day-after [[_ ^Long n ^Long after]]
  (nth-biz-day-from n after false))

(defmethod spec->date :nth-day [[_ ^Long n]]
  (nth-day-nxt-month n))

(defmethod spec->date :daily [_]
  (t/plus (today-at-start-of-day) (t/days 1)))

(defmethod spec->date :biz-daily [[_ & [_ ^Long lag-days]]]
  (next-biz-day (t/now) lag-days))

(defmethod spec->date :nth-week-day [[_ ^Long n]]
  (as-> (t/day-of-week (today-at-start-of-day)) x
    (t/days (+ n (- 7 x)))
    (t/plus (today-at-start-of-day) x)))

(defmethod spec->date :quarterly [_]
  (first-day-nxt-quarter))

;think '2nd friday every month' ==> n is 5, m is 2
(defmethod spec->date :nth-day-mth-week [[_ ^Long n ^Long m]]
  (let [first-of-month (nth-day-nxt-month 1)
        week (dec m)
        day (mod (- n (t/day-of-week first-of-month)) 7)]
    (t/plus first-of-month (t/weeks week) (t/days day))))

(defn next-available-date [spec]
  (-> spec
      spec->date
      t/with-time-at-start-of-day))


;;;;;;    PREVIOUS PERIOD DEFS  ;;;;;;
(defmulti prev-period (fn [published-on-id & _] published-on-id))

(defmethod prev-period :monthly [& _]
  (t/minus (t/now) (t/months 1)))

(defmethod prev-period :weekly [& _]
  (t/minus (t/now) (t/weeks 1)))

(defmethod prev-period :daily [& _]
  (t/minus (t/now) (t/days 1)))

(defmethod prev-period :biz-daily [_ [_ opt n]]
  (let [lag (or (when (= "lag-days" opt) n) 0)]
    (->> (t/now)
         (iterate #(t/minus % (t/days 1)))
         rest
         (filter #(biz-day? % lag))
         first)))

(defmethod prev-period :quarterly [& _]
  (t/minus (t/now) (t/months 3)))


;;;;;;   RETRY SPEC DEFS   ;;;;;;
(defmulti retry-at (fn [specs & _] (-> specs :type keyword)))

(defmethod retry-at :daily [{:keys [only-biz-days?]} min-tod]
  (cond-> (tomorrow min-tod)
    only-biz-days? (round-to-biz-day true)))

(defmethod retry-at :hourly [& _]
  (in-an-hour))

;;;;;;  UNIQUENESS DEFS   ;;;;;;
(defn job-type-dispatcher [{{job-type :type} :job-def} & _] (keyword job-type))

(defmulti unique? job-type-dispatcher)

(defmethod unique? :scrape [{{:keys [scraper-id params reschedule?]} :job-def id :_id}]
  (if (false? reschedule?)
    true
    (->> (if id {:_id {"$ne" id}} {})
         (merge {:job-def.scraper-id (name scraper-id)
                 :job-def.params     params
                 :run-after          {"$exists" true}
                 :job-def.reschedule? {"$exists" false}})
         (mc/count (dsc/get-db "mongo_jobq_configs") "queue")
         zero?)))

(defmethod unique? :daily-publication [{{:keys [schema tod publication-type]} :job-def retry-num :retry-num}]
  (zero? (mc/count (dsc/get-db "mongo_jobq_configs") "queue" {:job-def.type :daily-publication
                                                              :job-def.publication-type publication-type
                                                              :start-times     {"$exists" false}
                                                              :retry-num retry-num
                                                              :job-def.schema schema
                                                              :job-def.tod tod})))

(defmethod unique? :default [& _] true) ;do not enforce uniqueness by default

