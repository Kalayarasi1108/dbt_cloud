-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_THESIS_SUBMISSION (SAT_THESIS_SUBMISSION_SK,
                                                   HUB_THESIS_SUBMISSION_KEY,
                                                   SOURCE,
                                                   LOAD_DTS,
                                                   ETL_JOB_ID,
                                                   HASH_MD5,
                                                   STU_ID,
                                                   SSP_NO,
                                                   SSP_WRK_SUBM_NO,
                                                   EWS_DT,
                                                   WS_DT,
                                                   WS_REWORK_DT,
                                                   TECHONE_FLD1,
                                                   TECHONE_FLD2,
                                                   TECHONE_FLD3,
                                                   TECHONE_FLD4,
                                                   TECHONE_FLD5,
                                                   TECHONE_FLD6,
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
                                                   TECHONE_FLD7,
                                                   TECHONE_FLD8,
                                                   IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_THESIS_SUBMISSION_SK,
       MD5(IFNULL(STU_SPK.STU_ID, '') || ',' ||
           IFNULL(STU_SPK_WS.SSP_NO, 0) || ',' ||
           IFNULL(STU_SPK_WS.SSP_WRK_SUBM_NO, 0)
           )                                     HUB_THESIS_SUBMISSION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(STU_SPK.STU_ID, '') || ',' ||
           IFNULL(STU_SPK_WS.SSP_NO, 0) || ',' ||
           IFNULL(STU_SPK_WS.SSP_WRK_SUBM_NO, 0) || ',' ||
           IFNULL(STU_SPK_WS.EWS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_SPK_WS.WS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_SPK_WS.WS_REWORK_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_SPK_WS.TECHONE_FLD1, '') || ',' ||
           IFNULL(STU_SPK_WS.TECHONE_FLD2, '') || ',' ||
           IFNULL(STU_SPK_WS.TECHONE_FLD3, '') || ',' ||
           IFNULL(STU_SPK_WS.TECHONE_FLD4, '') || ',' ||
           IFNULL(STU_SPK_WS.TECHONE_FLD5, 0) || ',' ||
           IFNULL(STU_SPK_WS.TECHONE_FLD6, 0) || ',' ||
           IFNULL(STU_SPK_WS.VERS, 0) || ',' ||
           IFNULL(STU_SPK_WS.CRUSER, '') || ',' ||
           IFNULL(STU_SPK_WS.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_SPK_WS.CRTIMEI, 0) || ',' ||
           IFNULL(STU_SPK_WS.CRTERM, '') || ',' ||
           IFNULL(STU_SPK_WS.CRWINDOW, '') || ',' ||
           IFNULL(STU_SPK_WS.LAST_MOD_USER, '') || ',' ||
           IFNULL(STU_SPK_WS.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_SPK_WS.LAST_MOD_TIMEI, 0) || ',' ||
           IFNULL(STU_SPK_WS.LAST_MOD_TERM, '') || ',' ||
           IFNULL(STU_SPK_WS.LAST_MOD_WINDOW, '') || ',' ||
           IFNULL(STU_SPK_WS.TECHONE_FLD7, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_SPK_WS.TECHONE_FLD8, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           'N'
           )                                     HASH_MD5,
       STU_SPK.STU_ID,
       STU_SPK_WS.SSP_NO,
       STU_SPK_WS.SSP_WRK_SUBM_NO,
       STU_SPK_WS.EWS_DT,
       STU_SPK_WS.WS_DT,
       STU_SPK_WS.WS_REWORK_DT,
       STU_SPK_WS.TECHONE_FLD1,
       STU_SPK_WS.TECHONE_FLD2,
       STU_SPK_WS.TECHONE_FLD3,
       STU_SPK_WS.TECHONE_FLD4,
       STU_SPK_WS.TECHONE_FLD5,
       STU_SPK_WS.TECHONE_FLD6,
       STU_SPK_WS.VERS,
       STU_SPK_WS.CRUSER,
       STU_SPK_WS.CRDATEI,
       STU_SPK_WS.CRTIMEI,
       STU_SPK_WS.CRTERM,
       STU_SPK_WS.CRWINDOW,
       STU_SPK_WS.LAST_MOD_USER,
       STU_SPK_WS.LAST_MOD_DATEI,
       STU_SPK_WS.LAST_MOD_TIMEI,
       STU_SPK_WS.LAST_MOD_TERM,
       STU_SPK_WS.LAST_MOD_WINDOW,
       STU_SPK_WS.TECHONE_FLD7,
       STU_SPK_WS.TECHONE_FLD8,
       'N'                                       IS_DELETED
FROM ODS.AMIS.S1SSP_WRKSUBM_DTL STU_SPK_WS
         INNER JOIN ODS.AMIS.S1SSP_STU_SPK STU_SPK
                    ON STU_SPK_WS.SSP_NO = STU_SPK.SSP_NO
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_THESIS_SUBMISSION_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS)
                          OVER (PARTITION BY HUB_THESIS_SUBMISSION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_THESIS_SUBMISSION) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_THESIS_SUBMISSION_KEY = MD5(IFNULL(STU_SPK.STU_ID, '') || ',' ||
                                                IFNULL(STU_SPK_WS.SSP_NO, 0) || ',' ||
                                                IFNULL(STU_SPK_WS.SSP_WRK_SUBM_NO, 0)
            )
          AND S.HASH_MD5 = MD5(IFNULL(STU_SPK.STU_ID, '') || ',' ||
                               IFNULL(STU_SPK_WS.SSP_NO, 0) || ',' ||
                               IFNULL(STU_SPK_WS.SSP_WRK_SUBM_NO, 0) || ',' ||
                               IFNULL(STU_SPK_WS.EWS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(STU_SPK_WS.WS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(STU_SPK_WS.WS_REWORK_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(STU_SPK_WS.TECHONE_FLD1, '') || ',' ||
                               IFNULL(STU_SPK_WS.TECHONE_FLD2, '') || ',' ||
                               IFNULL(STU_SPK_WS.TECHONE_FLD3, '') || ',' ||
                               IFNULL(STU_SPK_WS.TECHONE_FLD4, '') || ',' ||
                               IFNULL(STU_SPK_WS.TECHONE_FLD5, 0) || ',' ||
                               IFNULL(STU_SPK_WS.TECHONE_FLD6, 0) || ',' ||
                               IFNULL(STU_SPK_WS.VERS, 0) || ',' ||
                               IFNULL(STU_SPK_WS.CRUSER, '') || ',' ||
                               IFNULL(STU_SPK_WS.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(STU_SPK_WS.CRTIMEI, 0) || ',' ||
                               IFNULL(STU_SPK_WS.CRTERM, '') || ',' ||
                               IFNULL(STU_SPK_WS.CRWINDOW, '') || ',' ||
                               IFNULL(STU_SPK_WS.LAST_MOD_USER, '') || ',' ||
                               IFNULL(STU_SPK_WS.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(STU_SPK_WS.LAST_MOD_TIMEI, 0) || ',' ||
                               IFNULL(STU_SPK_WS.LAST_MOD_TERM, '') || ',' ||
                               IFNULL(STU_SPK_WS.LAST_MOD_WINDOW, '') || ',' ||
                               IFNULL(STU_SPK_WS.TECHONE_FLD7, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(STU_SPK_WS.TECHONE_FLD8, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               'N'
            )
    );

