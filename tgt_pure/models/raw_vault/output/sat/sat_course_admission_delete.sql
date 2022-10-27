-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_COURSE_ADMISSION (SAT_COURSE_ADMISSION_SK, HUB_COURSE_ADMISSION_KEY, SOURCE, LOAD_DTS,
                                                  ETL_JOB_ID, HASH_MD5, STU_ID, SSP_NO, SPK_NO, SPK_VER_NO, SSP_ATT_NO,
                                                  SSP_STTS_NO, SSP_STG_CD, SSP_STTS_CD, EFFCT_START_DT, LOCATION_CD,
                                                  AVAIL_YR, SPRD_CD, AVAIL_NO, AVAIL_KEY_NO, LIAB_CAT_CD, LOAD_CAT_CD,
                                                  ATTNDC_MODE_CD, STUDY_MODE_CD, APPN_SPK_NO, APPN_VER_NO, APPN_NO,
                                                  CRDATEI, CRTIMEI, LAST_MOD_DATEI, LAST_MOD_TIMEI, STU_CONSOL_FG,
                                                  CONSOL_TO_STU_ID, REPAR_SSP_NO, REPAR_SPK_NO, REPAR_SPK_VER_NO,
                                                  REPAR_SSP_ATT_NO, STUDY_TYPE_CD, STUDY_BASIS_CD, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_ADMISSION_DATE_SK,
       S.HUB_COURSE_ADMISSION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       NULL                                      STU_ID,
       NULL                                      SSP_NO,
       NULL                                      SPK_NO,
       NULL                                      SPK_VER_NO,
       NULL                                      SSP_ATT_NO,
       NULL                                      SSP_STTS_NO,
       NULL                                      SSP_STG_CD,
       NULL                                      SSP_STTS_CD,
       NULL                                      EFFCT_START_DT,
       NULL                                      LOCATION_CD,
       NULL                                      AVAIL_YR,
       NULL                                      SPRD_CD,
       NULL                                      AVAIL_NO,
       NULL                                      AVAIL_KEY_NO,
       NULL                                      LIAB_CAT_CD,
       NULL                                      LOAD_CAT_CD,
       NULL                                      ATTNDC_MODE_CD,
       NULL                                      STUDY_MODE_CD,
       NULL                                      APPN_SPK_NO,
       NULL                                      APPN_VER_NO,
       NULL                                      APPN_NO,
       NULL                                      CRDATEI,
       NULL                                      CRTIMEI,
       NULL                                      LAST_MOD_DATEI,
       NULL                                      LAST_MOD_TIMEI,
       NULL                                      STU_CONSOL_FG,
       NULL                                      CONSOL_TO_STU_ID,
       NULL                                      REPAR_SSP_NO,
       NULL                                      REPAR_SPK_NO,
       NULL                                      REPAR_SPK_VER_NO,
       NULL                                      REPAR_SSP_ATT_NO,
       NULL                                      STUDY_TYPE_CD,
       NULL                                      STUDY_BASIS_CD,
       'Y'                                       IS_DELETED

FROM (
         SELECT HUB_COURSE_ADMISSION_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_ADMISSION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_COURSE_ADMISSION
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                 JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                      ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                          AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
        WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
          AND S.HUB_COURSE_ADMISSION_KEY = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.SSP_NO, 0)
            )
    )
;