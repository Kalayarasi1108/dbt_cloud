INSERT INTO DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_STATUS (SAT_UNIT_ENROLMENT_STATUS_SK,
                                                       HUB_UNIT_ENROLMENT_STATUS_KEY,
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
                                                       UNIT_ENROLMENT_STAGE,
                                                       SSP_STTS_CD,
                                                       UNIT_ENROLMENT_STATUS,
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
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_UNIT_ENROLMENT_STATUS_SK,
       MD5(IFNULL(UN_SSP_STTS.STU_ID, '') || ',' ||
           IFNULL(UN_SSP_STTS.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP_STTS.SSP_STTS_NO, 0)
           )                                     HUB_UNIT_ENROLMENT_STATUS_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(UN_SSP_STTS.STU_ID, '') || ',' ||
           IFNULL(UN_SSP_STTS.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP_STTS.SSP_STTS_NO, 0) || ',' ||
           IFNULL(UN_SSP_STTS.SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP_STTS.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP_STTS.SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP_STTS.SSP_STG_CD, '') || ',' ||
           IFNULL(UN_STAGE.CODE_DESCR, '') || ',' ||
           IFNULL(UN_SSP_STTS.SSP_STTS_CD, '') || ',' ||
           IFNULL(UN_STATUS.CODE_DESCR, '') || ',' ||
           IFNULL(UN_SSP_STTS.EFFCT_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP_STTS.EFFCT_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP_STTS.ENROL_TXN_NO, '') || ',' ||
           IFNULL(UN_SSP_STTS.VERS, 0) || ',' ||
           IFNULL(UN_SSP_STTS.CRUSER, '') || ',' ||
           IFNULL(UN_SSP_STTS.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP_STTS.CRTIMEI, 0) || ',' ||
           IFNULL(UN_SSP_STTS.CRTERM, '') || ',' ||
           IFNULL(UN_SSP_STTS.CRWINDOW, '') || ',' ||
           IFNULL(UN_SSP_STTS.LAST_MOD_USER, '') || ',' ||
           IFNULL(UN_SSP_STTS.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP_STTS.LAST_MOD_TIMEI, 0) || ',' ||
           IFNULL(UN_SSP_STTS.LAST_MOD_TERM, '') || ',' ||
           IFNULL(UN_SSP_STTS.LAST_MOD_WINDOW, '') || ',' ||
           IFNULL(UN_SSP_STTS.TECHONE_FLD1, '') || ',' ||
           IFNULL(UN_SSP_STTS.TECHONE_FLD2, '') || ',' ||
           IFNULL(UN_SSP_STTS.TECHONE_FLD3, '') || ',' ||
           IFNULL(UN_SSP_STTS.TECHONE_FLD4, '') || ',' ||
           IFNULL(UN_SSP_STTS.TECHONE_FLD5, 0) || ',' ||
           IFNULL(UN_SSP_STTS.TECHONE_FLD6, 0) || ',' ||
           IFNULL(UN_SSP_STTS.TECHONE_FLD7, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP_STTS.TECHONE_FLD8, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP_STTS.SSP_STTS_RSN_CD, '') || ',' ||
           IFNULL('N', '')
           )                                     HASH_MD5,
       UN_SSP_STTS.STU_ID,
       UN_SSP_STTS.SSP_NO,
       UN_SSP_STTS.SSP_STTS_NO,
       UN_SSP_STTS.SPK_NO,
       UN_SSP_STTS.SPK_VER_NO,
       UN_SSP_STTS.SSP_ATT_NO,
       UN_SSP_STTS.SSP_STG_CD,
       UN_STAGE.CODE_DESCR                       UNIT_ENROLMENT_STAGE,
       UN_SSP_STTS.SSP_STTS_CD,
       UN_STATUS.CODE_DESCR                      UNIT_ENROLMENT_STATUS,
       UN_SSP_STTS.EFFCT_START_DT,
       UN_SSP_STTS.EFFCT_END_DT,
       UN_SSP_STTS.ENROL_TXN_NO,
       UN_SSP_STTS.VERS,
       UN_SSP_STTS.CRUSER,
       UN_SSP_STTS.CRDATEI,
       UN_SSP_STTS.CRTIMEI,
       UN_SSP_STTS.CRTERM,
       UN_SSP_STTS.CRWINDOW,
       UN_SSP_STTS.LAST_MOD_USER,
       UN_SSP_STTS.LAST_MOD_DATEI,
       UN_SSP_STTS.LAST_MOD_TIMEI,
       UN_SSP_STTS.LAST_MOD_TERM,
       UN_SSP_STTS.LAST_MOD_WINDOW,
       UN_SSP_STTS.TECHONE_FLD1,
       UN_SSP_STTS.TECHONE_FLD2,
       UN_SSP_STTS.TECHONE_FLD3,
       UN_SSP_STTS.TECHONE_FLD4,
       UN_SSP_STTS.TECHONE_FLD5,
       UN_SSP_STTS.TECHONE_FLD6,
       UN_SSP_STTS.TECHONE_FLD7,
       UN_SSP_STTS.TECHONE_FLD8,
       UN_SSP_STTS.SSP_STTS_RSN_CD,
       'N'                                       IS_DELETED
FROM ODS.AMIS.S1SSP_STTS_HIST UN_SSP_STTS
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP_STTS.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP_STTS.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE UN_STAGE
                         ON UN_SSP_STTS.SSP_STG_CD = UN_STAGE.CODE_ID
                             AND UN_STAGE.CODE_TYPE = 'SSP_STG_CD'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE UN_STATUS
                         ON UN_SSP_STTS.SSP_STTS_CD = UN_STATUS.CODE_ID
                             AND UN_STATUS.CODE_TYPE = 'SSP_STTS_CD'
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_UNIT_ENROLMENT_STATUS_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS)
                          OVER (PARTITION BY HUB_UNIT_ENROLMENT_STATUS_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_STATUS) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_UNIT_ENROLMENT_STATUS_KEY =
              MD5(IFNULL(UN_SSP_STTS.STU_ID, '') || ',' ||
                  IFNULL(UN_SSP_STTS.SSP_NO, 0) || ',' ||
                  IFNULL(UN_SSP_STTS.SSP_STTS_NO, 0)
                  )
          AND S.HASH_MD5 = MD5(IFNULL(UN_SSP_STTS.STU_ID, '') || ',' ||
                               IFNULL(UN_SSP_STTS.SSP_NO, 0) || ',' ||
                               IFNULL(UN_SSP_STTS.SSP_STTS_NO, 0) || ',' ||
                               IFNULL(UN_SSP_STTS.SPK_NO, 0) || ',' ||
                               IFNULL(UN_SSP_STTS.SPK_VER_NO, 0) || ',' ||
                               IFNULL(UN_SSP_STTS.SSP_ATT_NO, 0) || ',' ||
                               IFNULL(UN_SSP_STTS.SSP_STG_CD, '') || ',' ||
                               IFNULL(UN_STAGE.CODE_DESCR, '') || ',' ||
                               IFNULL(UN_SSP_STTS.SSP_STTS_CD, '') || ',' ||
                               IFNULL(UN_STATUS.CODE_DESCR, '') || ',' ||
                               IFNULL(UN_SSP_STTS.EFFCT_START_DT,
                                      TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(UN_SSP_STTS.EFFCT_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(UN_SSP_STTS.ENROL_TXN_NO, '') || ',' ||
                               IFNULL(UN_SSP_STTS.VERS, 0) || ',' ||
                               IFNULL(UN_SSP_STTS.CRUSER, '') || ',' ||
                               IFNULL(UN_SSP_STTS.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(UN_SSP_STTS.CRTIMEI, 0) || ',' ||
                               IFNULL(UN_SSP_STTS.CRTERM, '') || ',' ||
                               IFNULL(UN_SSP_STTS.CRWINDOW, '') || ',' ||
                               IFNULL(UN_SSP_STTS.LAST_MOD_USER, '') || ',' ||
                               IFNULL(UN_SSP_STTS.LAST_MOD_DATEI,
                                      TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(UN_SSP_STTS.LAST_MOD_TIMEI, 0) || ',' ||
                               IFNULL(UN_SSP_STTS.LAST_MOD_TERM, '') || ',' ||
                               IFNULL(UN_SSP_STTS.LAST_MOD_WINDOW, '') || ',' ||
                               IFNULL(UN_SSP_STTS.TECHONE_FLD1, '') || ',' ||
                               IFNULL(UN_SSP_STTS.TECHONE_FLD2, '') || ',' ||
                               IFNULL(UN_SSP_STTS.TECHONE_FLD3, '') || ',' ||
                               IFNULL(UN_SSP_STTS.TECHONE_FLD4, '') || ',' ||
                               IFNULL(UN_SSP_STTS.TECHONE_FLD5, 0) || ',' ||
                               IFNULL(UN_SSP_STTS.TECHONE_FLD6, 0) || ',' ||
                               IFNULL(UN_SSP_STTS.TECHONE_FLD7, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(UN_SSP_STTS.TECHONE_FLD8, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(UN_SSP_STTS.SSP_STTS_RSN_CD, '') || ',' ||
                               IFNULL('N', '')
            )
    )
;

