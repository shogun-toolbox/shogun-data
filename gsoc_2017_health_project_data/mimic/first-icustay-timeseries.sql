-- This query pivots the time series vital signs for the first 24 hours of a patient's stay
-- Vital signs include heart rate, blood pressure, respiration rate, and temperature

set search_path to mimiciii;
WITH stg AS 
(
    SELECT pvt.subject_id,
    pvt.hadm_id,
    pvt.icustay_id,
    pvt."timestamp",
        CASE
            WHEN pvt.vitalid = 1 THEN pvt.valuenum
            ELSE NULL::double precision
        END AS heartrate,
        CASE
            WHEN pvt.vitalid = 2 THEN pvt.valuenum
            ELSE NULL::double precision
        END AS sysbp,
        CASE
            WHEN pvt.vitalid = 3 THEN pvt.valuenum
            ELSE NULL::double precision
        END AS diasbp,
        CASE
            WHEN pvt.vitalid = 4 THEN pvt.valuenum
            ELSE NULL::double precision
        END AS meanbp,
        CASE
            WHEN pvt.vitalid = 5 THEN pvt.valuenum
            ELSE NULL::double precision
        END AS resprate,
        CASE
            WHEN pvt.vitalid = 6 THEN pvt.valuenum
            ELSE NULL::double precision
        END AS tempc,
        CASE
            WHEN pvt.vitalid = 7 THEN pvt.valuenum
            ELSE NULL::double precision
        END AS spo2
    FROM ( SELECT ie.subject_id,
            ie.hadm_id,
            ie.icustay_id,
            date_trunc('hour'::text, ce.charttime) AS "timestamp",
                CASE
                    WHEN (ce.itemid = ANY (ARRAY[211, 220045])) AND ce.valuenum > 0::double precision AND ce.valuenum < 300::double precision THEN 1
                    WHEN (ce.itemid = ANY (ARRAY[51, 442, 455, 6701, 220179, 220050])) AND ce.valuenum > 0::double precision AND ce.valuenum < 400::double precision THEN 2
                    WHEN (ce.itemid = ANY (ARRAY[8368, 8440, 8441, 8555, 220180, 220051])) AND ce.valuenum > 0::double precision AND ce.valuenum < 300::double precision THEN 3
                    WHEN (ce.itemid = ANY (ARRAY[456, 52, 6702, 443, 220052, 220181, 225312])) AND ce.valuenum > 0::double precision AND ce.valuenum < 300::double precision THEN 4
                    WHEN (ce.itemid = ANY (ARRAY[615, 618, 220210, 224690])) AND ce.valuenum > 0::double precision AND ce.valuenum < 70::double precision THEN 5
                    WHEN (ce.itemid = ANY (ARRAY[223761, 678])) AND ce.valuenum > 70::double precision AND ce.valuenum < 120::double precision THEN 6
                    WHEN (ce.itemid = ANY (ARRAY[223762, 676])) AND ce.valuenum > 10::double precision AND ce.valuenum < 50::double precision THEN 6
                    WHEN (ce.itemid = ANY (ARRAY[646, 220277])) AND ce.valuenum > 0::double precision AND ce.valuenum <= 100::double precision THEN 7
                    ELSE NULL::integer
                END AS vitalid,
                CASE
                    WHEN ce.itemid = ANY (ARRAY[223761, 678]) THEN (ce.valuenum - 32::double precision) / 1.8::double precision
                    ELSE ce.valuenum
                END AS valuenum
            FROM mimiciii.icustays ie
                LEFT JOIN mimiciii.chartevents ce ON ie.subject_id = ce.subject_id AND ie.hadm_id = ce.hadm_id AND ie.icustay_id = ce.icustay_id AND ce.charttime >= ie.intime AND ce.charttime <= (ie.intime + '1 day'::interval day) AND ce.error IS DISTINCT FROM 1
            WHERE ce.itemid = ANY (ARRAY[211, 220045, 51, 442, 455, 6701, 220179, 220050, 8368, 8440, 8441, 8555, 220180, 220051, 456, 52, 6702, 443, 220052, 220181, 225312, 618, 615, 220210, 224690, 223761, 678, 223762, 676, 646, 220277])) pvt
    GROUP BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt."timestamp", pvt.vitalid, pvt.valuenum
    ORDER BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id
), agg AS 
(
    SELECT stg."timestamp",
    stg.subject_id,
    stg.hadm_id,
    stg.icustay_id,
    avg(stg.heartrate) AS heartrate,
    avg(stg.sysbp) AS sysbp,
    avg(stg.diasbp) AS diasbp,
    avg(stg.meanbp) AS meanbp,
    avg(stg.resprate) AS resprate,
    avg(stg.tempc) AS tempc,
    avg(stg.spo2) AS spo2
    FROM stg
    GROUP BY stg.icustay_id, stg.hadm_id, stg.subject_id, stg."timestamp"
    ORDER BY stg.icustay_id, stg.hadm_id, stg.subject_id, stg."timestamp"
)
, vitalsfirstday_timeseries AS 
(
    SELECT agg."timestamp",
    agg.subject_id,
    agg.hadm_id,
    agg.icustay_id,
    agg.heartrate AS hr,
    lag(agg.heartrate, 1) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS hr_1h,
    lag(agg.heartrate, 2) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS hr_2h,
    lag(agg.heartrate, 3) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS hr_3h,
    avg(agg.heartrate) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS hr_mean_6h,
    min(agg.heartrate) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS hr_min_6h,
    max(agg.heartrate) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS hr_max_6h,
    agg.sysbp,
    lag(agg.sysbp, 1) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS sysbp_1h,
    lag(agg.sysbp, 2) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS sysbp_2h,
    lag(agg.sysbp, 3) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS sysbp_3h,
    avg(agg.sysbp) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sysbp_mean_6h,
    min(agg.sysbp) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sysbp_min_6h,
    max(agg.sysbp) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sysbp_max_6h,
    agg.diasbp,
    lag(agg.diasbp, 1) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS diasbp_1h,
    lag(agg.diasbp, 2) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS diasbp_2h,
    lag(agg.diasbp, 3) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS diasbp_3h,
    avg(agg.diasbp) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS diasbp_mean_6h,
    min(agg.diasbp) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS diasbp_min_6h,
    max(agg.diasbp) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS diasbp_max_6h,
    agg.meanbp,
    lag(agg.meanbp, 1) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS meanbp_1h,
    lag(agg.meanbp, 2) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS meanbp_2h,
    lag(agg.meanbp, 3) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS meanbp_3h,
    avg(agg.meanbp) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS meanbp_mean_6h,
    min(agg.meanbp) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS meanbp_min_6h,
    max(agg.meanbp) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS meanbp_max_6h,
    agg.resprate,
    lag(agg.resprate, 1) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS resprate_1h,
    lag(agg.resprate, 2) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS resprate_2h,
    lag(agg.resprate, 3) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS resprate_3h,
    avg(agg.resprate) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS resprate_mean_6h,
    min(agg.resprate) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS resprate_min_6h,
    max(agg.resprate) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS resprate_max_6h,
    agg.tempc,
    lag(agg.tempc, 1) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS tempc_1h,
    lag(agg.tempc, 2) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS tempc_2h,
    lag(agg.tempc, 3) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS tempc_3h,
    avg(agg.tempc) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS tempc_mean_6h,
    min(agg.tempc) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS tempc_min_6h,
    max(agg.tempc) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS tempc_max_6h,
    agg.spo2,
    lag(agg.spo2, 1) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS spo2_1h,
    lag(agg.spo2, 2) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS spo2_2h,
    lag(agg.spo2, 3) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp") AS spo2_3h,
    avg(agg.spo2) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS spo2_mean_6h,
    min(agg.spo2) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS spo2_min_6h,
    max(agg.spo2) OVER (ORDER BY agg.icustay_id, agg.subject_id, agg."timestamp" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS spo2_max_6h
    FROM agg
    ORDER BY agg.subject_id, agg.hadm_id, agg.icustay_id, agg."timestamp"
)

-- Table : Services
, serv AS
(
SELECT icu.hadm_id, icu.icustay_id, se.curr_service
, CASE
    WHEN curr_service like '%SURG' then 1
    WHEN curr_service = 'ORTHO' then 1
    ELSE 0 END
  as surgical
, RANK() OVER (PARTITION BY icu.hadm_id ORDER BY se.transfertime DESC) as rank
FROM icustays icu
LEFT JOIN services se
 ON icu.hadm_id = se.hadm_id
AND se.transfertime < icu.intime + interval '12' hour
)

-- Table : Clinical data + demographics
, co AS
(
SELECT icu.subject_id, icu.hadm_id, icu.icustay_id, first_careunit, admission_type
, icu.los as icu_los
, round((EXTRACT(EPOCH FROM (adm.dischtime-adm.admittime))/60/60/24) :: NUMERIC, 4) as hosp_los
, EXTRACT('epoch' from icu.intime - pat.dob) / 60.0 / 60.0 / 24.0 / 365.242 as age_icu_in
, icu.intime as icu_intime
, pat.gender
, pat.dod_hosp
, RANK() OVER (PARTITION BY icu.subject_id ORDER BY icu.intime) AS icustay_id_order
, hospital_expire_flag
, CASE WHEN pat.dod IS NOT NULL 
       AND pat.dod >= icu.intime - interval '6 hour'
       AND pat.dod <= icu.outtime + interval '6 hour' THEN 1 
       ELSE 0 END AS icu_expire_flag
, CASE WHEN pat.dod IS NOT NULL
    AND pat.dod < adm.admittime + interval '30' day THEN 1 
    ELSE 0 END as hospital30day_expire_flag
, CASE WHEN pat.dod IS NOT NULL
    AND pat.dod < adm.admittime + interval '1' year THEN 1 
    ELSE 0 END as hospital1year_expire_flag      
FROM icustays icu
INNER JOIN patients pat
  ON icu.subject_id = pat.subject_id
INNER JOIN admissions adm
ON adm.hadm_id = icu.hadm_id    
)

-- Table : Exclusions
, excl AS
(
SELECT
  co.subject_id, co.hadm_id, co.icustay_id, co.icu_los, co.hosp_los
  , co.age_icu_in
  , co.gender
  , co.icustay_id_order
  , serv.curr_service
  , co.first_careunit
  , co.hospital_expire_flag
  , co.icu_expire_flag
  , CASE
        WHEN co.icu_los < 1 then 1
    ELSE 0 END
    AS exclusion_los
  , CASE
        WHEN co.age_icu_in < 16 then 1
    ELSE 0 END
    AS exclusion_age
  , CASE 
        WHEN co.icustay_id_order != 1 THEN 1
    ELSE 0 END 
    AS exclusion_first_stay
  , CASE
        WHEN serv.surgical = 1 THEN 1
    ELSE 0 END
    as exclusion_surgical
FROM co
LEFT JOIN serv
  ON  co.icustay_id = serv.icustay_id
  AND serv.rank = 1
)

SELECT 

-- vital signs for the first 24 hours of the icu stay
vital.*

-- vital.icustay_id, vital.subject_id, vital.hadm_id
-- , HeartRate
-- , DiasBP
-- , SysBP
-- , MeanBP
-- , RespRate
-- , TempC
-- , SpO2

-- demographic data
, co.age_icu_in, co.first_careunit, co.gender, co.admission_type
, EXTRACT('epoch' from vital.timestamp - co.icu_intime) / 60.0 / 60.0 / 24.0 / 365.242 as icu_los
-- outcomes
, co.hospital_expire_flag, co.icu_expire_flag
, co.hosp_los, co.icu_los, co.icustay_id_order
, co.dod_hosp
, CASE 
    WHEN vital.timestamp >= co.dod_hosp THEN 1 
    ELSE 0 END 
    AS dead
, CASE 
    WHEN vital.timestamp + interval '1' day >= co.dod_hosp THEN 1 
    ELSE 0 END 
    AS dead_in_1d
, CASE 
    WHEN vital.timestamp + interval '7' day >= co.dod_hosp THEN 1 
    ELSE 0 END 
    AS dead_in_7d
, CASE 
    WHEN vital.timestamp + interval '30' day >= co.dod_hosp THEN 1 
    ELSE 0 END 
    AS dead_in_30d

-- exclusions
, excl.exclusion_los, excl.exclusion_age
, excl.exclusion_first_stay, excl.exclusion_surgical

FROM vitalsfirstday_timeseries vital
left join co
  ON vital.icustay_id = co.icustay_id
left join excl
  on vital.icustay_id = excl.icustay_id;
