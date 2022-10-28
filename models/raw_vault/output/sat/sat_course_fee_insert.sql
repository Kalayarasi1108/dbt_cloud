-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_COURSE_FEE (SAT_COURSE_FEE_SK, HUB_COURSE_FEE_KEY, SOURCE, LOAD_DTS, ETL_JOB_ID,
                                            HASH_MD5, SPK_CD, SPK_VER_NO, FEE_LIAB_NO, FEE_YR, SPK_NO, FEE_NAME,
                                            FEE_DESC, FORM_NM, HDR_COURSE_BASE_FEE_AMOUNT, HDR_COURSE_FEE_AMOUNT,
                                            ATTENDENCE_MODE_CD, FORM_DESC, FORM_LINE1, FIN_CAT_NAME, FIN_CAT_TYPE,
                                            FIN_CAT_TYPE_DESC, FEE_GRP, FEE_START_DT, FEE_END_DT, LOCATION_CD,
                                            ORG_UNIT_CD, DISB_NO, DISB_YR, FEE_RULE_CAT_NO, FEE_RULE_CAT_YR,
                                            SPK_CAT_TYPE_CD, RESCH_CSWK_CD, SPK_CAT_TYPE_DESC, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL                                     SAT_COURSE_FEE_SK,
       MD5(IFNULL(SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(FEE_DET_1.FEE_LIAB_NO, 0) || ',' ||
           IFNULL(FEE_DET_1.FEE_YR, 0)
           )                                                           HUB_COURSE_FEE_KEY,
       'AMIS'                                                          SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ                       ETL_JOB_ID,
       MD5(
                   IFNULL(SPK_CD, '') || ',' ||
                   IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
                   IFNULL(FEE_DET_1.FEE_LIAB_NO, 0) || ',' ||
                   IFNULL(FEE_DET_1.FEE_YR, 0) || ',' ||
                   IFNULL(SPK_DET.SPK_NO, 0) || ',' ||
                   IFNULL(FEE_DET_1.FEE_NAME, '') || ',' ||
                   IFNULL(FEE_DET_1.FEE_DESC, '') || ',' ||
                   IFNULL(FEE_DET_1.FORM_NM, '') || ',' ||
                   IFNULL(IFF(FEE_DET_1.FORM_NM LIKE 'RR%',
                              REGEXP_REPLACE(FEE_DET_1.FORM_NM, '[A-Za-z]')::FLOAT, NULL), 0) || ',' ||
                   IFNULL(IFF(FEE_DET_1.FORM_NM LIKE 'RR%',
                              REGEXP_REPLACE(FEE_DET_1.FORM_NM, '[A-Za-z]')::FLOAT *
                              IFF(FEE_DET_1.FORM_NM LIKE '%X', 0.9, 1), NULL), 0) || ',' ||
                   IFNULL(IFF(FEE_DET_1.FORM_NM LIKE '%X', 'EXT', 'INT'), '') || ',' ||
                   IFNULL(FORMULA.FORM_DESC, '') || ',' ||
                   IFNULL(FORMULA.FORM_LINE1, '') || ',' ||
                   IFNULL(FEE_DET_1.FIN_CAT_NAME, '') || ',' ||
                   IFNULL(FEE_DET_1.FIN_CAT_TYPE, '') || ',' ||
                   IFNULL(FIN_CAT_TYPE_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(FEE_DET_1.FEE_GRP, '') || ',' ||
                   IFNULL(FEE_DET_1.FEE_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(FEE_DET_1.FEE_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(FEE_DET_1.LOCATION_CD, '') || ',' ||
                   IFNULL(FEE_DET_1.ORG_UNIT_CD, '') || ',' ||
                   IFNULL(FEE_DET_1.DISB_NO, 0) || ',' ||
                   IFNULL(FEE_DET_1.DISB_YR, 0) || ',' ||
                   IFNULL(FEE_DET_1.FEE_RULE_CAT_NO, 0) || ',' ||
                   IFNULL(FEE_DET_1.FEE_RULE_CAT_YR, 0) || ',' ||
                   IFNULL(CAT_TYPE.SPK_CAT_TYPE_CD, '') || ',' ||
                   IFNULL(CAT_TYPE.RESCH_CSWK_CD, '') || ',' ||
                   IFNULL(CAT_TYPE.SPK_CAT_TYPE_DESC, '') || ',' ||
                   'N'
           )                                                           HASH_MD5,
       SPK_DET.SPK_CD,
       SPK_DET.SPK_VER_NO,
       FEE_DET_1.FEE_LIAB_NO,
       FEE_DET_1.FEE_YR,
       SPK_DET.SPK_NO,
       FEE_DET_1.FEE_NAME,
       FEE_DET_1.FEE_DESC,
       FEE_DET_1.FORM_NM,
       IFF(FEE_DET_1.FORM_NM LIKE 'RR%',
           REGEXP_REPLACE(FEE_DET_1.FORM_NM, '[A-Za-z]')::FLOAT, NULL) HDR_COURSE_BASE_FEE_AMOUNT,
       IFF(FEE_DET_1.FORM_NM LIKE 'RR%',
           REGEXP_REPLACE(FEE_DET_1.FORM_NM, '[A-Za-z]')::FLOAT *
           IFF(FEE_DET_1.FORM_NM LIKE '%X', 0.9, 1), NULL)             HDR_COURSE_FEE_AMOUNT,
       IFF(FEE_DET_1.FORM_NM LIKE '%X', 'EXT', 'INT')                  ATTENDENCE_MODE_CD,
       FORMULA.FORM_DESC,
       FORMULA.FORM_LINE1,
       FEE_DET_1.FIN_CAT_NAME,
       FEE_DET_1.FIN_CAT_TYPE,
       FIN_CAT_TYPE_CODE.CODE_DESCR                                    FIN_CAT_TYPE_DESC,
       FEE_DET_1.FEE_GRP,
       FEE_DET_1.FEE_START_DT,
       FEE_DET_1.FEE_END_DT,
       FEE_DET_1.LOCATION_CD,
       FEE_DET_1.ORG_UNIT_CD,
       FEE_DET_1.DISB_NO,
       FEE_DET_1.DISB_YR,
       FEE_DET_1.FEE_RULE_CAT_NO,
       FEE_DET_1.FEE_RULE_CAT_YR,
       CAT_TYPE.SPK_CAT_TYPE_CD,
       CAT_TYPE.RESCH_CSWK_CD,
       CAT_TYPE.SPK_CAT_TYPE_DESC,
       'N'                                                             IS_DELETED
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
         JOIN ODS.AMIS.S1FOR_FORMULA FORMULA
              ON FORMULA.FORM_NM = FEE_DET_1.FORM_NM
                  AND FORMULA.FORM_YR = FEE_DET_1.FORM_YR
         JOIN ODS.AMIS.S1CAT_TYPE CAT_TYPE
              ON CAT_TYPE.SPK_CAT_TYPE_CD = SPK_DET.SPK_CAT_TYPE_CD
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE FIN_CAT_TYPE_CODE
                         ON FIN_CAT_TYPE_CODE.CODE_ID = FEE_DET_1.FIN_CAT_TYPE
                             AND FIN_CAT_TYPE_CODE.CODE_TYPE = 'FIN_CAT_TYPE'
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_COURSE_FEE_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_FEE_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_COURSE_FEE) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_COURSE_FEE_KEY = MD5(IFNULL(SPK_DET.SPK_CD, '') || ',' ||
                                         IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
                                         IFNULL(FEE_DET_1.FEE_LIAB_NO, 0) || ',' ||
                                         IFNULL(FEE_DET_1.FEE_YR, 0)
            )
          AND S.HASH_MD5 = MD5(
                    IFNULL(SPK_CD, '') || ',' ||
                    IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
                    IFNULL(FEE_DET_1.FEE_LIAB_NO, 0) || ',' ||
                    IFNULL(FEE_DET_1.FEE_YR, 0) || ',' ||
                    IFNULL(SPK_DET.SPK_NO, 0) || ',' ||
                    IFNULL(FEE_DET_1.FEE_NAME, '') || ',' ||
                    IFNULL(FEE_DET_1.FEE_DESC, '') || ',' ||
                    IFNULL(FEE_DET_1.FORM_NM, '') || ',' ||
                    IFNULL(IFF(FEE_DET_1.FORM_NM LIKE 'RR%',
                               REGEXP_REPLACE(FEE_DET_1.FORM_NM, '[A-Za-z]')::FLOAT, NULL), 0) || ',' ||
                    IFNULL(IFF(FEE_DET_1.FORM_NM LIKE 'RR%',
                               REGEXP_REPLACE(FEE_DET_1.FORM_NM, '[A-Za-z]')::FLOAT *
                               IFF(FEE_DET_1.FORM_NM LIKE '%X', 0.9, 1), NULL), 0) || ',' ||
                    IFNULL(IFF(FEE_DET_1.FORM_NM LIKE '%X', 'EXT', 'INT'), '') || ',' ||
                    IFNULL(FORMULA.FORM_DESC, '') || ',' ||
                    IFNULL(FORMULA.FORM_LINE1, '') || ',' ||
                    IFNULL(FEE_DET_1.FIN_CAT_NAME, '') || ',' ||
                    IFNULL(FEE_DET_1.FIN_CAT_TYPE, '') || ',' ||
                    IFNULL(FIN_CAT_TYPE_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(FEE_DET_1.FEE_GRP, '') || ',' ||
                    IFNULL(FEE_DET_1.FEE_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(FEE_DET_1.FEE_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(FEE_DET_1.LOCATION_CD, '') || ',' ||
                    IFNULL(FEE_DET_1.ORG_UNIT_CD, '') || ',' ||
                    IFNULL(FEE_DET_1.DISB_NO, 0) || ',' ||
                    IFNULL(FEE_DET_1.DISB_YR, 0) || ',' ||
                    IFNULL(FEE_DET_1.FEE_RULE_CAT_NO, 0) || ',' ||
                    IFNULL(FEE_DET_1.FEE_RULE_CAT_YR, 0) || ',' ||
                    IFNULL(CAT_TYPE.SPK_CAT_TYPE_CD, '') || ',' ||
                    IFNULL(CAT_TYPE.RESCH_CSWK_CD, '') || ',' ||
                    IFNULL(CAT_TYPE.SPK_CAT_TYPE_DESC, '') || ',' ||
                    'N'
            )
    )
;

