-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_COURSE_ADMISSION (SAT_COURSE_ADMISSION_SK, HUB_COURSE_ADMISSION_KEY, SOURCE, LOAD_DTS,
                                                  ETL_JOB_ID, HASH_MD5, STU_ID, SSP_NO, SPK_NO, SPK_VER_NO, SSP_ATT_NO,
                                                  SSP_STTS_NO, SSP_STG_CD, SSP_STTS_CD, EFFCT_START_DT, LOCATION_CD,
                                                  AVAIL_YR, SPRD_CD, AVAIL_NO, AVAIL_KEY_NO, LIAB_CAT_CD, LOAD_CAT_CD,
                                                  ATTNDC_MODE_CD, STUDY_MODE_CD, APPN_SPK_NO, APPN_VER_NO, APPN_NO,
                                                  CRDATEI, CRTIMEI, LAST_MOD_DATEI, LAST_MOD_TIMEI, STU_CONSOL_FG,
                                                  CONSOL_TO_STU_ID, REPAR_SSP_NO, REPAR_SPK_NO, REPAR_SPK_VER_NO,
                                                  REPAR_SSP_ATT_NO, STUDY_TYPE_CD, STUDY_BASIS_CD, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_ADMISSION_SK,
       MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
           IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_NO, 0)
           )                                     HUB_COURSE_ADMISSION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
           IFNULL(CS_SSP.SSP_NO, 0) || ',' ||
           IFNULL(CS_SSP.SPK_NO, 0) || ',' ||
           IFNULL(CS_SSP.SPK_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_ATT_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_STTS_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_STG_CD, '') || ',' ||
           IFNULL(CS_SSP.SSP_STTS_CD, '') || ',' ||
           IFNULL(CS_SSP.EFFCT_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
           IFNULL(CS_SSP.AVAIL_NO, 0) || ',' ||
           IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(CS_SSP.LIAB_CAT_CD, '') || ',' ||
           IFNULL(CS_SSP.LOAD_CAT_CD, '') || ',' ||
           IFNULL(CS_SSP.ATTNDC_MODE_CD, '') || ',' ||
           IFNULL(CS_SSP.STUDY_MODE_CD, '') || ',' ||
           IFNULL(CS_SSP.APPN_SPK_NO, 0) || ',' ||
           IFNULL(CS_SSP.APPN_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP.APPN_NO, 0) || ',' ||
           IFNULL(CS_SSP.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(CS_SSP.CRTIMEI, 0) || ',' ||
           IFNULL(CS_SSP.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(CS_SSP.LAST_MOD_TIMEI, 0) || ',' ||
           IFNULL(CS_SSP.STU_CONSOL_FG, '') || ',' ||
           IFNULL(CS_SSP.CONSOL_TO_STU_ID, '') || ',' ||
           IFNULL(CS_SSP.REPAR_SSP_NO, 0) || ',' ||
           IFNULL(CS_SSP.REPAR_SPK_NO, 0) || ',' ||
           IFNULL(CS_SSP.REPAR_SPK_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP.REPAR_SSP_ATT_NO, 0) || ',' ||
           IFNULL(CS_SSP.STUDY_TYPE_CD, '') || ',' ||
           IFNULL(CS_SSP.STUDY_BASIS_CD, '') || ',' ||
           'N'
           )                                     HASH_MD5,
       CS_SSP.STU_ID,
       CS_SSP.SSP_NO,
       CS_SSP.SPK_NO,
       CS_SSP.SPK_VER_NO,
       CS_SSP.SSP_ATT_NO,
       CS_SSP.SSP_STTS_NO,
       CS_SSP.SSP_STG_CD,
       CS_SSP.SSP_STTS_CD,
       CS_SSP.EFFCT_START_DT,
       CS_SSP.LOCATION_CD,
       CS_SSP.AVAIL_YR,
       CS_SSP.SPRD_CD,
       CS_SSP.AVAIL_NO,
       CS_SSP.AVAIL_KEY_NO,
       CS_SSP.LIAB_CAT_CD,
       CS_SSP.LOAD_CAT_CD,
       CS_SSP.ATTNDC_MODE_CD,
       CS_SSP.STUDY_MODE_CD,
       CS_SSP.APPN_SPK_NO,
       CS_SSP.APPN_VER_NO,
       CS_SSP.APPN_NO,
       CS_SSP.CRDATEI,
       CS_SSP.CRTIMEI,
       CS_SSP.LAST_MOD_DATEI,
       CS_SSP.LAST_MOD_TIMEI,
       CS_SSP.STU_CONSOL_FG,
       CS_SSP.CONSOL_TO_STU_ID,
       CS_SSP.REPAR_SSP_NO,
       CS_SSP.REPAR_SPK_NO,
       CS_SSP.REPAR_SPK_VER_NO,
       CS_SSP.REPAR_SSP_ATT_NO,
       CS_SSP.STUDY_TYPE_CD,
       CS_SSP.STUDY_BASIS_CD,
       'N'                                       IS_DELETED
FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
         JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
              ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                  AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_COURSE_ADMISSION_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_ADMISSION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_COURSE_ADMISSION) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_COURSE_ADMISSION_KEY = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.SSP_NO, 0)
            )
          AND S.HASH_MD5 = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                               IFNULL(CS_SSP.SSP_NO, 0) || ',' ||
                               IFNULL(CS_SSP.SPK_NO, 0) || ',' ||
                               IFNULL(CS_SSP.SPK_VER_NO, 0) || ',' ||
                               IFNULL(CS_SSP.SSP_ATT_NO, 0) || ',' ||
                               IFNULL(CS_SSP.SSP_STTS_NO, 0) || ',' ||
                               IFNULL(CS_SSP.SSP_STG_CD, '') || ',' ||
                               IFNULL(CS_SSP.SSP_STTS_CD, '') || ',' ||
                               IFNULL(CS_SSP.EFFCT_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                               IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                               IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                               IFNULL(CS_SSP.AVAIL_NO, 0) || ',' ||
                               IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                               IFNULL(CS_SSP.LIAB_CAT_CD, '') || ',' ||
                               IFNULL(CS_SSP.LOAD_CAT_CD, '') || ',' ||
                               IFNULL(CS_SSP.ATTNDC_MODE_CD, '') || ',' ||
                               IFNULL(CS_SSP.STUDY_MODE_CD, '') || ',' ||
                               IFNULL(CS_SSP.APPN_SPK_NO, 0) || ',' ||
                               IFNULL(CS_SSP.APPN_VER_NO, 0) || ',' ||
                               IFNULL(CS_SSP.APPN_NO, 0) || ',' ||
                               IFNULL(CS_SSP.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(CS_SSP.CRTIMEI, 0) || ',' ||
                               IFNULL(CS_SSP.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(CS_SSP.LAST_MOD_TIMEI, 0) || ',' ||
                               IFNULL(CS_SSP.STU_CONSOL_FG, '') || ',' ||
                               IFNULL(CS_SSP.CONSOL_TO_STU_ID, '') || ',' ||
                               IFNULL(CS_SSP.REPAR_SSP_NO, 0) || ',' ||
                               IFNULL(CS_SSP.REPAR_SPK_NO, 0) || ',' ||
                               IFNULL(CS_SSP.REPAR_SPK_VER_NO, 0) || ',' ||
                               IFNULL(CS_SSP.REPAR_SSP_ATT_NO, 0) || ',' ||
                               IFNULL(CS_SSP.STUDY_TYPE_CD, '') || ',' ||
                               IFNULL(CS_SSP.STUDY_BASIS_CD, '') || ',' ||
                               'N'
            )
    );

