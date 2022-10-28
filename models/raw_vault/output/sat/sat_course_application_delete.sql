-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_COURSE_APPLICATION (SAT_COURSE_APPLICATION_SK,
                                                                   HUB_COURSE_APPLICATION_KEY, SOURCE,
                                                                   LOAD_DTS, ETL_JOB_ID, HASH_MD5, STU_ID, SPK_NO,
                                                                   SPK_VER_NO, APPN_NO,
                                                                   SSP_NO, APPN_TYPE_CD, ADMSN_CNTR_CRS_CD,
                                                                   ADMSN_CNTR_APP_ID,
                                                                   LIAB_CAT_CD, LOAD_CAT_CD, ATTNDC_MODE_CD,
                                                                   STUDY_MODE_CD, APPN_DT,
                                                                   ASST_DUE_DT, APPN_STTS_NO, APPN_STG_CD, APPN_STTS_CD,
                                                                   OWNING_ORG_UNIT_CD, VERS, CRUSER, CRDATEI, CRTIMEI,
                                                                   CRTERM,
                                                                   CRWINDOW, LAST_MOD_USER, LAST_MOD_DATEI,
                                                                   LAST_MOD_TIMEI,
                                                                   LAST_MOD_TERM, LAST_MOD_WINDOW, APPN_PREF_NO,
                                                                   OFFER_PREF_NO,
                                                                   INCOMP_APPN_FG, TECHONE_FLD1, TECHONE_FLD2,
                                                                   TECHONE_FLD3,
                                                                   TECHONE_FLD4, TECHONE_FLD5, TECHONE_FLD6,
                                                                   TECHONE_FLD7,
                                                                   TECHONE_FLD8, PARENT_SSP_NO, GV1_EXCLUDE_FG,
                                                                   GV1_APP_ID, GV1_REF_DT,
                                                                   UPFRONT_PAYMNT_AMT, UPFRONT_TAX_AMT,
                                                                   UPFRONT_PAYMNT_ACK_FG,
                                                                   UPFRONT_PAYMNT_CALC_DATE, UPFRONT_PAYMNT_CALC_TIME,
                                                                   UPFRONT_PAYMNT_ACK_DATE, UPFRONT_PAYMNT_ACK_TIME,
                                                                   UPFRONT_WEB_PAYMNT_TYPE, UPFRONT_PAYMNT_REF_NO,
                                                                   UPFRONT_BANK_REF_NO,
                                                                   UPFRONT_CONFIRM_NO, EAP_APPN_SUBM_DT,
                                                                   EAP_APPN_SUBM_TM,
                                                                   APPLICATION_ID, APPLICATION_LINE_ID, OFFER_ROUND,
                                                                   STRUCTURE_ID,
                                                                   IS_DELETED,
                                                                   SUBMISSION_DT,
                                                                   SUBMISSION_METHOD_CD,
                                                                   SUBMISSION_METHOD)

SELECT DATA_VAULT.CORE.SEQ.NEXTVAL SAT_COURSE_APPLICATION_SK,
       MD5(S.HUB_COURSE_APPLICATION_KEY)             HUB_COURSE_APPLICATION_KEY,
       'AMIS'                                        SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ              LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ     ETL_JOB_ID,
       MD5('')                                       HASH_MD5,
       NULL                                          STU_ID,
       NULL                                          SPK_NO,
       NULL                                          SPK_VER_NO,
       NULL                                          APPN_NO,
       NULL                                          SSP_NO,
       NULL                                          APPN_TYPE_CD,
       NULL                                          ADMSN_CNTR_CRS_CD,
       NULL                                          ADMSN_CNTR_APP_ID,
       NULL                                          LIAB_CAT_CD,
       NULL                                          LOAD_CAT_CD,
       NULL                                          ATTNDC_MODE_CD,
       NULL                                          STUDY_MODE_CD,
       NULL                                          APPN_DT,
       NULL                                          ASST_DUE_DT,
       NULL                                          APPN_STTS_NO,
       NULL                                          APPN_STG_CD,
       NULL                                          APPN_STTS_CD,
       NULL                                          OWNING_ORG_UNIT_CD,
       NULL                                          VERS,
       NULL                                          CRUSER,
       NULL                                          CRDATEI,
       NULL                                          CRTIMEI,
       NULL                                          CRTERM,
       NULL                                          CRWINDOW,
       NULL                                          LAST_MOD_USER,
       NULL                                          LAST_MOD_DATEI,
       NULL                                          LAST_MOD_TIMEI,
       NULL                                          LAST_MOD_TERM,
       NULL                                          LAST_MOD_WINDOW,
       NULL                                          APPN_PREF_NO,
       NULL                                          OFFER_PREF_NO,
       NULL                                          INCOMP_APPN_FG,
       NULL                                          TECHONE_FLD1,
       NULL                                          TECHONE_FLD2,
       NULL                                          TECHONE_FLD3,
       NULL                                          TECHONE_FLD4,
       NULL                                          TECHONE_FLD5,
       NULL                                          TECHONE_FLD6,
       NULL                                          TECHONE_FLD7,
       NULL                                          TECHONE_FLD8,
       NULL                                          PARENT_SSP_NO,
       NULL                                          GV1_EXCLUDE_FG,
       NULL                                          GV1_APP_ID,
       NULL                                          GV1_REF_DT,
       NULL                                          UPFRONT_PAYMNT_AMT,
       NULL                                          UPFRONT_TAX_AMT,
       NULL                                          UPFRONT_PAYMNT_ACK_FG,
       NULL                                          UPFRONT_PAYMNT_CALC_DATE,
       NULL                                          UPFRONT_PAYMNT_CALC_TIME,
       NULL                                          UPFRONT_PAYMNT_ACK_DATE,
       NULL                                          UPFRONT_PAYMNT_ACK_TIME,
       NULL                                          UPFRONT_WEB_PAYMNT_TYPE,
       NULL                                          UPFRONT_PAYMNT_REF_NO,
       NULL                                          UPFRONT_BANK_REF_NO,
       NULL                                          UPFRONT_CONFIRM_NO,
       NULL                                          EAP_APPN_SUBM_DT,
       NULL                                          EAP_APPN_SUBM_TM,
       NULL                                          APPLICATION_ID,
       NULL                                          APPLICATION_LINE_ID,
       NULL                                          OFFER_ROUND,
       NULL                                          STRUCTURE_ID,
       'Y'                                           IS_DELETED,
       NULL                                          SUBMISSION_DT,
       NULL                                          SUBMISSION_METHOD_CD,
       NULL                                          SUBMISSION_METHOD
FROM (
         SELECT HUB.STU_ID,
                HUB.SPK_NO,
                HUB.SPK_VER_NO,
                HUB.APPN_NO::VARCHAR(200) AS                                                      APPN_NO,
                SAT.HUB_COURSE_APPLICATION_KEY,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_COURSE_APPLICATION_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_COURSE_APPLICATION SAT
                  JOIN DATA_VAULT.CORE.HUB_COURSE_APPLICATION HUB
                       ON HUB.HUB_COURSE_APPLICATION_KEY = SAT.HUB_COURSE_APPLICATION_KEY AND HUB.SOURCE = 'AMIS'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'

  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1STU_APPLICATION APP
        WHERE APP.STU_ID = S.STU_ID
          AND APP.SPK_NO = S.SPK_NO
          AND APP.SPK_VER_NO = S.SPK_VER_NO
          AND APP.APPN_NO::varchar(200) = S.APPN_NO
        UNION ALL
        SELECT NULL
        FROM ODS.AMIS.S1APP_APPLICATION as a
                 INNER JOIN ODS.AMIS.S1APP_APPLICATION_LINE as l on a.application_id = l.application_id
                 INNER JOIN ODS.AMIS.S1APP_STUDY as ls on l.application_id = ls.application_id
            and ls.application_line_id = l.application_line_id
                 LEFT JOIN ODS.AMIS.S1APP_OFFER as o on o.application_id = l.application_id
            and l.application_line_id = o.application_line_id

        WHERE ls.STU_ID = S.STU_ID
          AND ls.SPK_NO = S.SPK_NO
          AND ls.SPK_VER_NO = S.SPK_VER_NO
          AND CONCAT(ls.APPLICATION_ID, '_', ls.APPLICATION_LINE_ID) = S.APPN_NO
    );