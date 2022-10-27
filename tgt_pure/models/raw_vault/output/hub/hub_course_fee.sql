INSERT INTO DATA_VAULT.CORE.HUB_COURSE_FEE (HUB_COURSE_FEE_KEY, SPK_CD, SPK_VER_NO, FEE_LIAB_NO, FEE_YR,
                                            SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(IFNULL(SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(FEE_DET_1.FEE_LIAB_NO, 0) || ',' ||
           IFNULL(FEE_DET_1.FEE_YR, 0)
           )                                     HUB_COURSE_FEE_KEY,
       SPK_DET.SPK_CD,
       SPK_DET.SPK_VER_NO,
       FEE_DET_1.FEE_LIAB_NO,
       FEE_DET_1.FEE_YR,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SPK_FEE SPK_FEE
         JOIN ODS.AMIS.S1SPK_DET SPK_DET
              ON SPK_DET.SPK_NO = SPK_FEE.SPK_NO
                  AND SPK_DET.SPK_VER_NO = SPK_FEE.SPK_VER_NO
                  AND SPK_DET.SPK_CAT_CD = 'CS'
         JOIN ODS.AMIS.S1FEE_DET FEE_DET
              ON SPK_FEE.FEE_LIAB_NO = FEE_DET.FEE_LIAB_NO
                  AND SPK_FEE.FEE_YR = FEE_DET.FEE_YR
         JOIN ODS.AMIS.S1FEE_ASSOC_FEE FEE_ASSOC_FEE
              ON FEE_ASSOC_FEE.FEE_LIAB_NO = FEE_DET.FEE_LIAB_NO
                  AND FEE_ASSOC_FEE.FEE_YR = FEE_DET.FEE_YR
         JOIN ODS.AMIS.S1FEE_DET FEE_DET_1
              ON FEE_DET_1.FEE_LIAB_NO = FEE_ASSOC_FEE.ASSOC_FEE_LIAB_NO
                  AND FEE_DET_1.FEE_YR = FEE_ASSOC_FEE.ASSOC_FEE_YR
         WHERE NOT EXISTS(
              SELECT NULL
              FROM DATA_VAULT.CORE.HUB_COURSE_FEE H
              WHERE H.HUB_COURSE_FEE_KEY = MD5(IFNULL(SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(FEE_DET_1.FEE_LIAB_NO, 0) || ',' ||
                                               IFNULL(FEE_DET_1.FEE_YR, 0)
                  )
          )
;