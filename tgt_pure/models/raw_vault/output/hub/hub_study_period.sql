SELECT MD5(IFNULL(CYR_LOC_DET.CALDR_YR, 0) || ',' ||
           IFNULL(CYR_LOC_DET.LOCATION_CD, '') || ',' ||
           IFNULL(CYR_LOC_DET.SPRD_CD, '')
           )                                     HUB_STUDY_PERIOD_KEY,
       CYR_LOC_DET.CALDR_YR,
       CYR_LOC_DET.LOCATION_CD,
       CYR_LOC_DET.SPRD_CD,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM {{source('AMIS','S1CYR_LOC_DET')}} CYR_LOC_DET
WHERE NOT EXISTS(
        SELECT NULL
        FROM {{source('CORE','HUB_STUDY_PERIOD')}} HSP
        WHERE HSP.HUB_STUDY_PERIOD_KEY =
              MD5(IFNULL(CYR_LOC_DET.CALDR_YR, 0) || ',' ||
                  IFNULL(CYR_LOC_DET.LOCATION_CD, '') || ',' ||
                  IFNULL(CYR_LOC_DET.SPRD_CD, '')
                  )
    )
;