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
        WHEN itemid = 50868 THEN 'ANION GAP'
        WHEN itemid = 50862 THEN 'ALBUMIN'
        WHEN itemid = 51144 THEN 'BANDS'
        WHEN itemid = 50882 THEN 'BICARBONATE'
        WHEN itemid = 50885 THEN 'BILIRUBIN'
        WHEN itemid = 50912 THEN 'CREATININE'
        WHEN itemid = 50806 THEN 'CHLORIDE'
        WHEN itemid = 50902 THEN 'CHLORIDE'
        WHEN itemid = 50809 THEN 'GLUCOSE'
        WHEN itemid = 50931 THEN 'GLUCOSE'
        WHEN itemid = 50810 THEN 'HEMATOCRIT'
        WHEN itemid = 51221 THEN 'HEMATOCRIT'
        WHEN itemid = 50811 THEN 'HEMOGLOBIN'
        WHEN itemid = 51222 THEN 'HEMOGLOBIN'
        WHEN itemid = 50813 THEN 'LACTATE'
        WHEN itemid = 51265 THEN 'PLATELET'
        WHEN itemid = 50822 THEN 'POTASSIUM'
        WHEN itemid = 50971 THEN 'POTASSIUM'
        WHEN itemid = 51275 THEN 'PTT'
        WHEN itemid = 51237 THEN 'INR'
        WHEN itemid = 51274 THEN 'PT'
        WHEN itemid = 50824 THEN 'SODIUM'
        WHEN itemid = 50983 THEN 'SODIUM'
        WHEN itemid = 51006 THEN 'BUN'
        WHEN itemid = 51300 THEN 'WBC'
        WHEN itemid = 51301 THEN 'WBC'
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
          50868, -- ANION GAP | CHEMISTRY | BLOOD | 769895
          50862, -- ALBUMIN | CHEMISTRY | BLOOD | 146697
          51144, -- BANDS - hematology
          50882, -- BICARBONATE | CHEMISTRY | BLOOD | 780733
          50885, -- BILIRUBIN, TOTAL | CHEMISTRY | BLOOD | 238277
          50912, -- CREATININE | CHEMISTRY | BLOOD | 797476
          50902, -- CHLORIDE | CHEMISTRY | BLOOD | 795568
          50806, -- CHLORIDE, WHOLE BLOOD | BLOOD GAS | BLOOD | 48187
          50931, -- GLUCOSE | CHEMISTRY | BLOOD | 748981
          50809, -- GLUCOSE | BLOOD GAS | BLOOD | 196734
          51221, -- HEMATOCRIT | HEMATOLOGY | BLOOD | 881846
          50810, -- HEMATOCRIT, CALCULATED | BLOOD GAS | BLOOD | 89715
          51222, -- HEMOGLOBIN | HEMATOLOGY | BLOOD | 752523
          50811, -- HEMOGLOBIN | BLOOD GAS | BLOOD | 89712
          50813, -- LACTATE | BLOOD GAS | BLOOD | 187124
          51265, -- PLATELET COUNT | HEMATOLOGY | BLOOD | 778444
          50971, -- POTASSIUM | CHEMISTRY | BLOOD | 845825
          50822, -- POTASSIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 192946
          51275, -- PTT | HEMATOLOGY | BLOOD | 474937
          51237, -- INR(PT) | HEMATOLOGY | BLOOD | 471183
          51274, -- PT | HEMATOLOGY | BLOOD | 469090
          50983, -- SODIUM | CHEMISTRY | BLOOD | 808489
          50824, -- SODIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 71503
          51006, -- UREA NITROGEN | CHEMISTRY | BLOOD | 791925
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
    , min(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END) as ANIONGAP_min
    , max(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END) as ANIONGAP_max
    , min(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN_min
    , max(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN_max
    , min(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END) as BANDS_min
    , max(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END) as BANDS_max
    , min(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE_min
    , max(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE_max
    , min(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END) as BILIRUBIN_min
    , max(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END) as BILIRUBIN_max
    , min(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END) as CREATININE_min
    , max(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END) as CREATININE_max
    , min(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END) as CHLORIDE_min
    , max(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END) as CHLORIDE_max
    , min(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END) as GLUCOSE_min
    , max(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END) as GLUCOSE_max
    , min(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END) as HEMATOCRIT_min
    , max(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END) as HEMATOCRIT_max
    , min(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN_min
    , max(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN_max
    , min(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END) as LACTATE_min
    , max(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END) as LACTATE_max
    , min(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END) as PLATELET_min
    , max(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END) as PLATELET_max
    , min(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END) as POTASSIUM_min
    , max(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END) as POTASSIUM_max
    , min(CASE WHEN label = 'PTT' THEN valuenum ELSE null END) as PTT_min
    , max(CASE WHEN label = 'PTT' THEN valuenum ELSE null END) as PTT_max
    , min(CASE WHEN label = 'INR' THEN valuenum ELSE null END) as INR_min
    , max(CASE WHEN label = 'INR' THEN valuenum ELSE null END) as INR_max
    , min(CASE WHEN label = 'PT' THEN valuenum ELSE null END) as PT_min
    , max(CASE WHEN label = 'PT' THEN valuenum ELSE null END) as PT_max
    , min(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null END) as SODIUM_min
    , max(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null end) as SODIUM_max
    , min(CASE WHEN label = 'BUN' THEN valuenum ELSE null end) as BUN_min
    , max(CASE WHEN label = 'BUN' THEN valuenum ELSE null end) as BUN_max
    , min(CASE WHEN label = 'WBC' THEN valuenum ELSE null end) as WBC_min
    , max(CASE WHEN label = 'WBC' THEN valuenum ELSE null end) as WBC_max

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
, pat.dod_hosp
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
, HeartRate_Min, HeartRate_Mean, HeartRate_Max
, DiasBP_Min, DiasBP_Max, DiasBP_Mean
, SysBP_Min, SysBP_Max, SysBP_Mean
, MeanBP_Min, MeanBP_Max, MeanBP_Mean
, RespRate_Min, RespRate_Mean, RespRate_Max
, TempC_Min, TempC_Max, TempC_Mean
, SpO2_Min, SpO2_Max, SpO2_Mean

, GCS_Min, GCS_Max

-- lab values
, ANIONGAP_min, ANIONGAP_max
, ALBUMIN_min, ALBUMIN_max
, BANDS_min, BANDS_max
, BICARBONATE_min, BICARBONATE_max
, BILIRUBIN_min, BILIRUBIN_max
, CREATININE_min, CREATININE_max
, CHLORIDE_min, CHLORIDE_max
, GLUCOSE_min, GLUCOSE_max
, HEMATOCRIT_min, HEMATOCRIT_max
, HEMOGLOBIN_min, HEMOGLOBIN_max
, LACTATE_min, LACTATE_max
, PLATELET_min, PLATELET_max
, POTASSIUM_min, POTASSIUM_max
, PTT_min, PTT_max
, INR_min, INR_max
, PT_min, PT_max
, SODIUM_min, SODIUM_max
, BUN_min, BUN_max
, WBC_min, WBC_max

, co.age_icu_in, co.first_careunit, co.gender
, co.hospital_expire_flag, co.icu_expire_flag
, co.hosp_los, co.icu_los, co.icustay_id_order
, co.dod_hosp

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