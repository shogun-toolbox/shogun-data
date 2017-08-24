set search_path to mimiciii;

-- Staging table #1: CHARTEVENTS
with ce_stg as
(
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  , case
      when itemid in (211,220045) and chart.valuenum > 0 and chart.valuenum < 300 then 1 -- HeartRate
      when itemid in (51, 442, 455, 6701, 220179, 220050) and chart.valuenum > 0 and chart.valuenum < 400 then 2 -- SysBP
      when itemid in (8368, 8440, 8441, 8555, 220180, 220051) and chart.valuenum > 0 and chart.valuenum < 300 then 3 -- DiasBP
      when itemid in (456,52,6702,443,220052,220181,225312) and chart.valuenum > 0 and chart.valuenum < 300 then 4 -- MeanBP
      when itemid in (615,618,220210,224690) and chart.valuenum > 0 and chart.valuenum < 70 then 5 -- RespRate
      when itemid in (223761, 678) and chart.valuenum > 70 and chart.valuenum < 120 then 6 -- TempC
      when itemid in (223762, 676) and chart.valuenum > 10 and chart.valuenum < 50 then 6 -- TempC
      when itemid in (646, 220277) and chart.valuenum > 0 and chart.valuenum <= 100 then 7 -- SpO2
      when itemid in (807, 811, 1529, 3745, 3744, 225664, 220621, 226537) and chart.valuenum > 0 then 8 -- Glucose
      else null end as vitalid
  , case
      when chart.itemid = any (ARRAY[223761, 678]) then (chart.valuenum - 32::double precision) / 1.8::double precision
      else chart.valuenum end as valuenum

  from icustays ie
  left join chartevents chart
    on ie.subject_id = chart.subject_id and ie.hadm_id = chart.hadm_id and ie.icustay_id = chart.icustay_id
    and chart.charttime >= ie.intime and chart.charttime <= (ie.intime + '1 day'::interval day)
    and chart.error is distinct from 1
    where chart.itemid = any
    (array[
    -- HEART RATE
    211, --"Heart Rate"
    220045, --"Heart Rate"

    -- SYSTOLIC BLOOD PRESSURE
    51, 
    442, 
    455, 
    6701, 
    220179, 
    220050,

    -- DIASTOLIC BLOOD PRESSURE
    8368, 
    8440, 
    8441, 
    8555, 
    220180, 
    220051,

    -- MEAN BLOOD PRESSURE
    456, --"NBP Mean"
    52, --"Arterial BP Mean"
    6702, --	Arterial BP Mean #2
    443, --	Manual BP Mean(calc)
    220052, --"Arterial Blood Pressure mean"
    220181, --"Non Invasive Blood Pressure mean"
    225312, --"ART BP mean"

    -- RESPIRATORY RATE
    618,--	Respiratory Rate
    615,--	Resp Rate (Total)
    220210,--	Respiratory Rate
    224690, --	Respiratory Rate (Total)

    -- TEMP C
    223761,
    678,
    223762, 
    676,

    -- SpO2
    646, 
    220277,

    -- GLUCOSE
    807, 
    811, 
    1529, 
    3745, 
    3744, 
    225664, 
    220621, 
    226537
    ])
)
-- Aggregate table #1: CHARTEVENTS
, ce as
(
  SELECT ce_stg.subject_id, ce_stg.hadm_id, ce_stg.icustay_id
  , min(case when VitalID = 1 then valuenum else null end) as HeartRate_Min
  , max(case when VitalID = 1 then valuenum else null end) as HeartRate_Max
  , avg(case when VitalID = 1 then valuenum else null end) as HeartRate_Mean
  , min(case when VitalID = 2 then valuenum else null end) as SysBP_Min
  , max(case when VitalID = 2 then valuenum else null end) as SysBP_Max
  , avg(case when VitalID = 2 then valuenum else null end) as SysBP_Mean
  , min(case when VitalID = 3 then valuenum else null end) as DiasBP_Min
  , max(case when VitalID = 3 then valuenum else null end) as DiasBP_Max
  , avg(case when VitalID = 3 then valuenum else null end) as DiasBP_Mean
  , min(case when VitalID = 4 then valuenum else null end) as MeanBP_Min
  , max(case when VitalID = 4 then valuenum else null end) as MeanBP_Max
  , avg(case when VitalID = 4 then valuenum else null end) as MeanBP_Mean
  , min(case when VitalID = 5 then valuenum else null end) as RespRate_Min
  , max(case when VitalID = 5 then valuenum else null end) as RespRate_Max
  , avg(case when VitalID = 5 then valuenum else null end) as RespRate_Mean
  , min(case when VitalID = 6 then valuenum else null end) as TempC_Min
  , max(case when VitalID = 6 then valuenum else null end) as TempC_Max
  , avg(case when VitalID = 6 then valuenum else null end) as TempC_Mean
  , min(case when VitalID = 7 then valuenum else null end) as SpO2_Min
  , max(case when VitalID = 7 then valuenum else null end) as SpO2_Max
  , avg(case when VitalID = 7 then valuenum else null end) as SpO2_Mean
  FROM ce_stg
  group by ce_stg.subject_id, ce_stg.hadm_id, ce_stg.icustay_id
  order by ce_stg.subject_id, ce_stg.hadm_id, ce_stg.icustay_id
   
)

-- Staging table #2: GCS
-- Because we need to add together GCS components, we do it seperately from chartevents
, gcs_stg as
(
  select ie.icustay_id, chart.charttime
  , max(case when itemid in (723,223900) then valuenum else null end) as GCSVerbal
  , max(case when itemid in (454,223901) then valuenum else null end) as GCSMotor
  , max(case when itemid in (184,220739) then valuenum else null end) as GCSEyes
  from icustays ie
  left join chartevents chart
    on ie.subject_id = chart.subject_id and ie.hadm_id = chart.hadm_id and ie.icustay_id = chart.icustay_id
    and chart.charttime >= ie.intime and chart.charttime <= (ie.intime + '1 day'::interval day)
    and chart.itemid in
    (
      723, -- GCSVerbal
      454, -- GCSMotor
      184, -- GCSEyes
      223900, -- GCS - Verbal Response
      223901, -- GCS - Motor Response
      220739 -- GCS - Eye Opening
    )
  group by ie.icustay_id, chart.charttime
)
-- Aggregate table #2: GCS
, gcs as
(
  SELECT gcs_stg.icustay_id
  , min(GCSVerbal + GCSMotor + GCSEyes) as GCS_Min
  , max(GCSVerbal + GCSMotor + GCSEyes) as GCS_Max
  FROM gcs_stg
  group by gcs_stg.icustay_id
)
-- Staging table #3: LABEVENTS
, le_stg as
(
  select ie.icustay_id
    -- here we assign labels to ITEMIDs
    -- this also fuses together multiple ITEMIDs containing the same data
    , case
          when itemid = 50885 then 'BILIRUBIN'
          when itemid = 50912 then 'CREATININE'
          when itemid = 50809 then 'GLUCOSE'
          when itemid = 50931 then 'GLUCOSE'
          when itemid = 50811 then 'HEMOGLOBIN'
          when itemid = 51222 then 'HEMOGLOBIN'
          when itemid = 50824 then 'SODIUM'
          when itemid = 50983 then 'SODIUM'
          when itemid = 51300 then 'WBC'
          when itemid = 51301 then 'WBC'
        else null
      end as label
    , valuenum

    from icustays ie

    left join labevents lab
      on ie.subject_id = lab.subject_id and ie.hadm_id = lab.hadm_id
      and lab.charttime >= (ie.intime - '6 hour'::interval hour) and lab.charttime <= (ie.intime + '1 day'::interval day)
      and lab.ITEMID in
      (
        -- comment is: LABEL | CATEGORY | FLUID | NUMBER OF ROWS IN LABEVENTS
        50885, -- BILIRUBIN, TOTAL | CHEMISTRY | BLOOD | 238277
        50912, -- CREATININE | CHEMISTRY | BLOOD | 797476
        50931, -- GLUCOSE | CHEMISTRY | BLOOD | 748981
        50809, -- GLUCOSE | BLOOD GAS | BLOOD | 196734
        51222, -- HEMOGLOBIN | HEMATOLOGY | BLOOD | 752523
        50811, -- HEMOGLOBIN | BLOOD GAS | BLOOD | 89712
        50983, -- SODIUM | CHEMISTRY | BLOOD | 808489
        50824, -- SODIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 71503
        51301, -- WHITE BLOOD CELLS | HEMATOLOGY | BLOOD | 753301
        51300  -- WBC COUNT | HEMATOLOGY | BLOOD | 2371
      )
      and lab.valuenum is not null and lab.valuenum > 0 -- lab values cannot be 0 and cannot be negative
)

-- Aggregate table #3: LABEVENTS
, le as
(
  select
    le_stg.icustay_id

    , min(case when label = 'BILIRUBIN' then valuenum else null end) as BILIRUBIN_min
    , max(case when label = 'BILIRUBIN' then valuenum else null end) as BILIRUBIN_max
    , min(case when label = 'CREATININE' then valuenum else null end) as CREATININE_min
    , max(case when label = 'CREATININE' then valuenum else null end) as CREATININE_max
    , min(case when label = 'HEMOGLOBIN' then valuenum else null end) as HEMOGLOBIN_min
    , max(case when label = 'HEMOGLOBIN' then valuenum else null end) as HEMOGLOBIN_max
    , min(case when label = 'SODIUM' then valuenum else null end) as SODIUM_min
    , max(case when label = 'SODIUM' then valuenum else null end) as SODIUM_max
    , min(case when label = 'WBC' then valuenum else null end) as WBC_min
    , max(case when label = 'WBC' then valuenum else null end) as WBC_max

  from le_stg
  group by le_stg.icustay_id
)

-- Table #3: Services
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

-- Table #4: Clinical data + demographics
, co AS
(
SELECT icu.subject_id, icu.hadm_id, icu.icustay_id, first_careunit
, icu.los as icu_los
, round((EXTRACT(EPOCH FROM (adm.dischtime-adm.admittime))/60/60/24) :: NUMERIC, 4) as hosp_los
, EXTRACT('epoch' from icu.intime - pat.dob) / 60.0 / 60.0 / 24.0 / 365.242 as age_icu_in
, pat.gender
, RANK() OVER (PARTITION BY icu.subject_id ORDER BY icu.intime) AS icustay_id_order
, hospital_expire_flag
, CASE WHEN pat.dod IS NOT NULL 
       AND pat.dod >= icu.intime - interval '6 hour'
       AND pat.dod <= icu.outtime + interval '6 hour' THEN 1 
       ELSE 0 END AS icu_expire_flag
FROM icustays icu
INNER JOIN patients pat
  ON icu.subject_id = pat.subject_id
INNER JOIN admissions adm
ON adm.hadm_id = icu.hadm_id    
)

-- Table #5: Exclusions
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

SELECT ie.icustay_id, ie.subject_id, ie.hadm_id
-- , adm.HOSPITAL_EXPIRE_FLAG -- whether the patient died within the hospital
-- , EXTRACT('epoch' from ie.intime - pat.dob) / 60.0 / 60.0 / 24.0 / 365.242 as Age
, HeartRate_Min, HeartRate_Mean, HeartRate_Max
, DiasBP_Min, DiasBP_Max, DiasBP_Mean
, SysBP_Min, SysBP_Max, SysBP_Mean
, MeanBP_Min, MeanBP_Max, MeanBP_Mean
, RespRate_Min, RespRate_Mean, RespRate_Max
, TempC_Min, TempC_Max, TempC_Mean
, SpO2_Min, SpO2_Max, SpO2_Mean

, GCS_Min, GCS_Max

, BILIRUBIN_min, BILIRUBIN_max
, CREATININE_min, CREATININE_max
, HEMOGLOBIN_min, HEMOGLOBIN_max
, SODIUM_min, SODIUM_max
, WBC_min, WBC_max

, co.age_icu_in, co.first_careunit, co.gender
, co.hospital_expire_flag, co.icu_expire_flag
, co.hosp_los, co.icu_los, co.icustay_id_order

FROM icustays ie
inner join admissions adm
  on ie.hadm_id = adm.hadm_id
inner join patients pat
  on ie.subject_id = pat.subject_id
left join ce
  on ie.icustay_id = ce.icustay_id
left join gcs
  on ie.icustay_id = gcs.icustay_id
left join le
  on ie.icustay_id = le.icustay_id
left join co
  on ie.icustay_id = co.icustay_id
left join excl
  on ie.icustay_id = excl.icustay_id