INSERT INTO DATA_VAULT.CORE.SAT_COURSE_ADMISSION_STATUS (SAT_COURSE_ADMISSION_STATUS_SK,
                                                         HUB_COURSE_ADMISSION_STATUS_KEY,
                                                         SOURCE,
                                                         LOAD_DTS,
                                                         ETL_JOB_ID,
                                                         HASH_MD5,
                                                         STU_ID,
                                                         SSP_NO,
                                                         SSP_STTS_NO,
                                                         SPK_NO,
                                                         SPK_VER_NO,
                                                         SSP_ATT_NO,
                                                         SSP_STG_CD,
                                                         COURSE_ADMISSION_STAGE,
                                                         SSP_STTS_CD,
                                                         COURSE_ADMISSION_STATUS,
                                                         EFFCT_START_DT,
                                                         EFFCT_END_DT,
                                                         ENROL_TXN_NO,
                                                         VERS,
                                                         CRUSER,
                                                         CRDATEI,
                                                         CRTIMEI,
                                                         CRTERM,
                                                         CRWINDOW,
                                                         LAST_MOD_USER,
                                                         LAST_MOD_DATEI,
                                                         LAST_MOD_TIMEI,
                                                         LAST_MOD_TERM,
                                                         LAST_MOD_WINDOW,
                                                         TECHONE_FLD1,
                                                         TECHONE_FLD2,
                                                         TECHONE_FLD3,
                                                         TECHONE_FLD4,
                                                         TECHONE_FLD5,
                                                         TECHONE_FLD6,
                                                         TECHONE_FLD7,
                                                         TECHONE_FLD8,
                                                         SSP_STTS_RSN_CD,
                                                         IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_ADMISSION_STATUS_SK,
       MD5(IFNULL(CS_SSP_STTS.STU_ID, '') || ',' ||
           IFNULL(CS_SSP_STTS.SSP_NO, 0) || ',' ||
           IFNULL(CS_SSP_STTS.SSP_STTS_NO, 0)
           )                                     HUB_COURSE_ADMISSION_STATUS_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(CS_SSP_STTS.STU_ID, '') || ',' ||
           IFNULL(CS_SSP_STTS.SSP_NO, 0) || ',' ||
           IFNULL(CS_SSP_STTS.SSP_STTS_NO, 0) || ',' ||
           IFNULL(CS_SSP_STTS.SPK_NO, 0) || ',' ||
           IFNULL(CS_SSP_STTS.SPK_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP_STTS.SSP_ATT_NO, 0) || ',' ||
           IFNULL(CS_SSP_STTS.SSP_STG_CD, '') || ',' ||
           IFNULL(CS_STAGE.CODE_DESCR, '') || ',' ||
           IFNULL(CS_SSP_STTS.SSP_STTS_CD, '') || ',' ||
           IFNULL(CS_STATUS.CODE_DESCR, '') || ',' ||
           IFNULL(CS_SSP_STTS.EFFCT_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(CS_SSP_STTS.EFFCT_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(CS_SSP_STTS.ENROL_TXN_NO, '') || ',' ||
           IFNULL(CS_SSP_STTS.VERS, 0) || ',' ||
           IFNULL(CS_SSP_STTS.CRUSER, '') || ',' ||
           IFNULL(CS_SSP_STTS.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(CS_SSP_STTS.CRTIMEI, 0) || ',' ||
           IFNULL(CS_SSP_STTS.CRTERM, '') || ',' ||
           IFNULL(CS_SSP_STTS.CRWINDOW, '') || ',' ||
           IFNULL(CS_SSP_STTS.LAST_MOD_USER, '') || ',' ||
           IFNULL(CS_SSP_STTS.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(CS_SSP_STTS.LAST_MOD_TIMEI, 0) || ',' ||
           IFNULL(CS_SSP_STTS.LAST_MOD_TERM, '') || ',' ||
           IFNULL(CS_SSP_STTS.LAST_MOD_WINDOW, '') || ',' ||
           IFNULL(CS_SSP_STTS.TECHONE_FLD1, '') || ',' ||
           IFNULL(CS_SSP_STTS.TECHONE_FLD2, '') || ',' ||
           IFNULL(CS_SSP_STTS.TECHONE_FLD3, '') || ',' ||
           IFNULL(CS_SSP_STTS.TECHONE_FLD4, '') || ',' ||
           IFNULL(CS_SSP_STTS.TECHONE_FLD5, 0) || ',' ||
           IFNULL(CS_SSP_STTS.TECHONE_FLD6, 0) || ',' ||
           IFNULL(CS_SSP_STTS.TECHONE_FLD7, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(CS_SSP_STTS.TECHONE_FLD8, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(CS_SSP_STTS.SSP_STTS_RSN_CD, '') || ',' ||
           IFNULL('N', '')
           )                                     HASH_MD5,
       CS_SSP_STTS.STU_ID,
       CS_SSP_STTS.SSP_NO,
       CS_SSP_STTS.SSP_STTS_NO,
       CS_SSP_STTS.SPK_NO,
       CS_SSP_STTS.SPK_VER_NO,
       CS_SSP_STTS.SSP_ATT_NO,
       CS_SSP_STTS.SSP_STG_CD,
       CS_STAGE.CODE_DESCR                       COURSE_ADMISSION_STAGE,
       CS_SSP_STTS.SSP_STTS_CD,
       CS_STATUS.CODE_DESCR                      COURSE_ADMISSION_STATUS,
       CS_SSP_STTS.EFFCT_START_DT,
       CS_SSP_STTS.EFFCT_END_DT,
       CS_SSP_STTS.ENROL_TXN_NO,
       CS_SSP_STTS.VERS,
       CS_SSP_STTS.CRUSER,
       CS_SSP_STTS.CRDATEI,
       CS_SSP_STTS.CRTIMEI,
       CS_SSP_STTS.CRTERM,
       CS_SSP_STTS.CRWINDOW,
       CS_SSP_STTS.LAST_MOD_USER,
       CS_SSP_STTS.LAST_MOD_DATEI,
       CS_SSP_STTS.LAST_MOD_TIMEI,
       CS_SSP_STTS.LAST_MOD_TERM,
       CS_SSP_STTS.LAST_MOD_WINDOW,
       CS_SSP_STTS.TECHONE_FLD1,
       CS_SSP_STTS.TECHONE_FLD2,
       CS_SSP_STTS.TECHONE_FLD3,
       CS_SSP_STTS.TECHONE_FLD4,
       CS_SSP_STTS.TECHONE_FLD5,
       CS_SSP_STTS.TECHONE_FLD6,
       CS_SSP_STTS.TECHONE_FLD7,
       CS_SSP_STTS.TECHONE_FLD8,
       CS_SSP_STTS.SSP_STTS_RSN_CD,
       'N'                                       IS_DELETED
FROM ODS.AMIS.S1SSP_STTS_HIST CS_SSP_STTS
         JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
              ON CS_SPK_DET.SPK_NO = CS_SSP_STTS.SPK_NO
                  AND CS_SPK_DET.SPK_VER_NO = CS_SSP_STTS.SPK_VER_NO
                  AND CS_SPK_DET.SPK_CAT_CD = 'CS'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE CS_STAGE
                         ON CS_SSP_STTS.SSP_STG_CD = CS_STAGE.CODE_ID
                             AND CS_STAGE.CODE_TYPE = 'SSP_STG_CD'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE CS_STATUS
                         ON CS_SSP_STTS.SSP_STTS_CD = CS_STATUS.CODE_ID
                             AND CS_STATUS.CODE_TYPE = 'SSP_STTS_CD'
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_COURSE_ADMISSION_STATUS_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS)
                          OVER (PARTITION BY HUB_COURSE_ADMISSION_STATUS_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_COURSE_ADMISSION_STATUS) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_COURSE_ADMISSION_STATUS_KEY =
              MD5(IFNULL(CS_SSP_STTS.STU_ID, '') || ',' ||
                  IFNULL(CS_SSP_STTS.SSP_NO, 0) || ',' ||
                  IFNULL(CS_SSP_STTS.SSP_STTS_NO, 0)
                  )
          AND S.HASH_MD5 = MD5(IFNULL(CS_SSP_STTS.STU_ID, '') || ',' ||
                               IFNULL(CS_SSP_STTS.SSP_NO, 0) || ',' ||
                               IFNULL(CS_SSP_STTS.SSP_STTS_NO, 0) || ',' ||
                               IFNULL(CS_SSP_STTS.SPK_NO, 0) || ',' ||
                               IFNULL(CS_SSP_STTS.SPK_VER_NO, 0) || ',' ||
                               IFNULL(CS_SSP_STTS.SSP_ATT_NO, 0) || ',' ||
                               IFNULL(CS_SSP_STTS.SSP_STG_CD, '') || ',' ||
                               IFNULL(CS_STAGE.CODE_DESCR, '') || ',' ||
                               IFNULL(CS_SSP_STTS.SSP_STTS_CD, '') || ',' ||
                               IFNULL(CS_STATUS.CODE_DESCR, '') || ',' ||
                               IFNULL(CS_SSP_STTS.EFFCT_START_DT,
                                      TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(CS_SSP_STTS.EFFCT_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(CS_SSP_STTS.ENROL_TXN_NO, '') || ',' ||
                               IFNULL(CS_SSP_STTS.VERS, 0) || ',' ||
                               IFNULL(CS_SSP_STTS.CRUSER, '') || ',' ||
                               IFNULL(CS_SSP_STTS.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(CS_SSP_STTS.CRTIMEI, 0) || ',' ||
                               IFNULL(CS_SSP_STTS.CRTERM, '') || ',' ||
                               IFNULL(CS_SSP_STTS.CRWINDOW, '') || ',' ||
                               IFNULL(CS_SSP_STTS.LAST_MOD_USER, '') || ',' ||
                               IFNULL(CS_SSP_STTS.LAST_MOD_DATEI,
                                      TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(CS_SSP_STTS.LAST_MOD_TIMEI, 0) || ',' ||
                               IFNULL(CS_SSP_STTS.LAST_MOD_TERM, '') || ',' ||
                               IFNULL(CS_SSP_STTS.LAST_MOD_WINDOW, '') || ',' ||
                               IFNULL(CS_SSP_STTS.TECHONE_FLD1, '') || ',' ||
                               IFNULL(CS_SSP_STTS.TECHONE_FLD2, '') || ',' ||
                               IFNULL(CS_SSP_STTS.TECHONE_FLD3, '') || ',' ||
                               IFNULL(CS_SSP_STTS.TECHONE_FLD4, '') || ',' ||
                               IFNULL(CS_SSP_STTS.TECHONE_FLD5, 0) || ',' ||
                               IFNULL(CS_SSP_STTS.TECHONE_FLD6, 0) || ',' ||
                               IFNULL(CS_SSP_STTS.TECHONE_FLD7, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(CS_SSP_STTS.TECHONE_FLD8, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(CS_SSP_STTS.SSP_STTS_RSN_CD, '') || ',' ||
                               IFNULL('N', '')
            )
    )
;

