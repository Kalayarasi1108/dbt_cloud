-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_COURSE_APPLICATION (SAT_COURSE_APPLICATION_SK,
                                                                  HUB_COURSE_APPLICATION_KEY,
                                                                  SOURCE,
                                                                  LOAD_DTS,
                                                                  ETL_JOB_ID,
                                                                  HASH_MD5,
                                                                  STU_ID,
                                                                  SPK_NO,
                                                                  SPK_VER_NO,
                                                                  APPN_NO,
                                                                  SSP_NO,
                                                                  APPN_TYPE_CD,
                                                                  ADMSN_CNTR_CRS_CD,
                                                                  ADMSN_CNTR_APP_ID,
                                                                  LIAB_CAT_CD,
                                                                  LOAD_CAT_CD,
                                                                  ATTNDC_MODE_CD,
                                                                  STUDY_MODE_CD,
                                                                  APPN_DT,
                                                                  ASST_DUE_DT,
                                                                  APPN_STTS_NO,
                                                                  APPN_STG_CD,
                                                                  APPN_STTS_CD,
                                                                  OWNING_ORG_UNIT_CD,
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
                                                                  APPN_PREF_NO,
                                                                  OFFER_PREF_NO,
                                                                  INCOMP_APPN_FG,
                                                                  TECHONE_FLD1,
                                                                  TECHONE_FLD2,
                                                                  TECHONE_FLD3,
                                                                  TECHONE_FLD4,
                                                                  TECHONE_FLD5,
                                                                  TECHONE_FLD6,
                                                                  TECHONE_FLD7,
                                                                  TECHONE_FLD8,
                                                                  PARENT_SSP_NO,
                                                                  GV1_EXCLUDE_FG,
                                                                  GV1_APP_ID,
                                                                  GV1_REF_DT,
                                                                  UPFRONT_PAYMNT_AMT,
                                                                  UPFRONT_TAX_AMT,
                                                                  UPFRONT_PAYMNT_ACK_FG,
                                                                  UPFRONT_PAYMNT_CALC_DATE,
                                                                  UPFRONT_PAYMNT_CALC_TIME,
                                                                  UPFRONT_PAYMNT_ACK_DATE,
                                                                  UPFRONT_PAYMNT_ACK_TIME,
                                                                  UPFRONT_WEB_PAYMNT_TYPE,
                                                                  UPFRONT_PAYMNT_REF_NO,
                                                                  UPFRONT_BANK_REF_NO,
                                                                  UPFRONT_CONFIRM_NO,
                                                                  EAP_APPN_SUBM_DT,
                                                                  EAP_APPN_SUBM_TM,
                                                                  APPLICATION_ID,
                                                                  APPLICATION_LINE_ID,
                                                                  OFFER_ROUND,
                                                                  STRUCTURE_ID,
                                                                  IS_DELETED,
                                                                  SUBMISSION_DT,
                                                                  SUBMISSION_METHOD_CD,
                                                                  SUBMISSION_METHOD)

SELECT SAT_COURSE_APPLICATION_SK,
       HUB_COURSE_APPLICATION_KEY,
       SOURCE,
       LOAD_DTS,
       ETL_JOB_ID,
       HASH_MD5,
       STU_ID,
       SPK_NO,
       SPK_VER_NO,
       APPN_NO,
       SSP_NO,
       APPN_TYPE_CD,
       ADMSN_CNTR_CRS_CD,
       ADMSN_CNTR_APP_ID,
       LIAB_CAT_CD,
       LOAD_CAT_CD,
       ATTNDC_MODE_CD,
       STUDY_MODE_CD,
       APPN_DT,
       ASST_DUE_DT,
       APPN_STTS_NO,
       APPN_STG_CD,
       APPN_STTS_CD,
       OWNING_ORG_UNIT_CD,
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
       APPN_PREF_NO,
       OFFER_PREF_NO,
       INCOMP_APPN_FG,
       TECHONE_FLD1,
       TECHONE_FLD2,
       TECHONE_FLD3,
       TECHONE_FLD4,
       TECHONE_FLD5,
       TECHONE_FLD6,
       TECHONE_FLD7,
       TECHONE_FLD8,
       PARENT_SSP_NO,
       GV1_EXCLUDE_FG,
       GV1_APP_ID,
       GV1_REF_DT,
       UPFRONT_PAYMNT_AMT,
       UPFRONT_TAX_AMT,
       UPFRONT_PAYMNT_ACK_FG,
       UPFRONT_PAYMNT_CALC_DATE,
       UPFRONT_PAYMNT_CALC_TIME,
       UPFRONT_PAYMNT_ACK_DATE,
       UPFRONT_PAYMNT_ACK_TIME,
       UPFRONT_WEB_PAYMNT_TYPE,
       UPFRONT_PAYMNT_REF_NO,
       UPFRONT_BANK_REF_NO,
       UPFRONT_CONFIRM_NO,
       EAP_APPN_SUBM_DT,
       EAP_APPN_SUBM_TM,
       APPLICATION_ID,
       APPLICATION_LINE_ID,
       OFFER_ROUND,
       STRUCTURE_ID,
       IS_DELETED,
       SUBMISSION_DT,
       SUBMISSION_METHOD_CD,
       SUBMISSION_METHOD
FROM (
         SELECT DATA_VAULT.CORE.SEQ.nextval                                    as SAT_COURSE_APPLICATION_SK,
                MD5(
                            IFNULL(s.STU_ID, '') || ',' ||
                            IFNULL(s.SPK_NO, 0) || ',' ||
                            IFNULL(s.SPK_VER_NO, 0) || ',' ||
                            IFNULL(CONCAT(s.APPLICATION_ID, '_', s.APPLICATION_LINE_ID), '')
                    )                                                                               HUB_COURSE_APPLICATION_KEY,
                'AMIS'                                                                           as Source,
                CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                                 as LOAD_DTS,
                'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ::varchar(100)                          as ETL_JOB_ID,
                MD5(
                            IFNULL(s.STU_ID, '') || ',' ||
                            IFNULL(s.SPK_NO, 0) || ',' ||
                            IFNULL(s.SPK_VER_NO, 0) || ',' ||
                            IFNULL(CONCAT(s.application_id, '_', s.application_line_id), '') || ',' ||
                            IFNULL(s.SSP_NO, 0) || ',' ||
                            IFNULL(a.submission_method_cd, '') || ',' ||
                            IFNULL(ADMISSION_CTR_COURSE_CD, '') || ',' ||
                            IFNULL(ADMISSION_CTR_APPLCTION_ID, '') || ',' ||
                            IFNULL(LIABILITY_CATEGORY_CD, '') || ',' ||
                            IFNULL(LOAD_CATEGORY_CD, '') || ',' ||
                            IFNULL(ATTENDANCE_MODE_CD, '') || ',' ||
                            IFNULL(s.STUDY_MODE_CD, '') || ',' ||
                            IFNULL(TO_CHAR(SUBMISSION_DT, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(TO_CHAR(ASSESSMENT_DUE_DT, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(NULL, 0) || ',' ||
                            IFNULL(NULL, '') || ',' ||
                            IFNULL(APPLICATION_STATUS_CD, '') || ',' ||
                            IFNULL(NULL, '') || ',' ||
                            IFNULL(s.VERS, 0) || ',' ||
                            IFNULL(s.CRUSER, '') || ',' ||
                            IFNULL(TO_CHAR(s.CRDATEI, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(s.CRTIMEI, 0) || ',' ||
                            IFNULL(s.CRTERM, '') || ',' ||
                            IFNULL(s.CRWINDOW, '') || ',' ||
                            IFNULL(s.LAST_MOD_USER, '') || ',' ||
                            IFNULL(TO_CHAR(s.LAST_MOD_DATEI, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(s.LAST_MOD_TIMEI, 0) || ',' ||
                            IFNULL(s.LAST_MOD_TERM, '') || ',' ||
                            IFNULL(s.LAST_MOD_WINDOW, '') || ',' ||
                            IFNULL(ADMISSION_CTR_PREFERNCE_NO, '') || ',' ||
                            IFNULL(PREFERENCE_NO::varchar(100), '') || ',' ||
                            IFNULL(INCOMPLETE_FG, '') || ',' ||
                            IFNULL(NULL, '') || ',' ||
                            IFNULL(NULL, '') || ',' ||
                            IFNULL(NULL, '') || ',' ||
                            IFNULL(NULL, '') || ',' ||
                            IFNULL(NULL, 0) || ',' ||
                            IFNULL(NULL, 0) || ',' ||
                            IFNULL(NULL, '') || ',' ||
                            IFNULL(NULL, '') || ',' ||
                            IFNULL(s.SSP_NO, 0) || ',' ||
                            IFNULL(s.GV1_EXCLUDE_FG, '') || ',' ||
                            IFNULL(GV1_APPLICATION_ID, '') || ',' ||
                            IFNULL(TO_CHAR(GV1_REFERENCE_DT, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(UPFRONT_PAYMENT_AMT, 0) || ',' ||
                            IFNULL(a.UPFRONT_TAX_AMT, 0) || ',' ||
                            IFNULL(NULL, '') || ',' ||
                            IFNULL(TO_CHAR(UPFRONT_PAYMNT_CALC_DT, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(NULL, 0) || ',' ||
                            IFNULL(TO_CHAR(UPFRONT_PAYMENT_ACK_DT, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(NULL, 0) || ',' ||
                            IFNULL(UPFRONT_WEB_PAYMENT_TYPE, '') || ',' ||
                            IFNULL(UPFRONT_PAYMENT_REF_NO, '') || ',' ||
                            IFNULL(a.UPFRONT_BANK_REF_NO, '') || ',' ||
                            IFNULL(UPFRONT_CONFIRMATION_NO, '') || ',' ||
                            IFNULL(NULL, 0) || ',' ||
                            IFNULL(NULL, 0) || ',' ||
                            IFNULL(s.APPLICATION_ID, 0) || ',' ||
                            IFNULL(s.APPLICATION_LINE_ID, 0) || ',' ||
                            IFNULL(o.OFFER_ROUND, '') || ',' ||
                            IFNULL(s.STRUCTURE_ID, 0) || ',' ||
                            IFNULL('N', '')
                    )                                                                               HASH_MD5,
                a.STU_ID,
                s.SPK_NO,
                s.SPK_VER_NO,
                CONCAT(l.application_id, '_', l.application_line_id)                             as APPN_NO,
                s.SSP_NO,
                submission_method_cd                                                             as APPN_TYPE_CD,
                x.admsn_cntr_crs_cd                                                              as ADMSN_CNTR_CRS_CD,
                x.admsn_cntr_app_id                                                              as ADMSN_CNTR_APP_ID,
                LIABILITY_CATEGORY_CD                                                            as LIAB_CAT_CD,
                LOAD_CATEGORY_CD                                                                 as LOAD_CAT_CD,
                ATTENDANCE_MODE_CD                                                               as ATTNDC_MODE_CD,
                s.STUDY_MODE_CD,
                --CASE WHEN SUBMISSION_METHOD_CD IN ('D','OS','SP') THEN SUBMISSION_DT ELSE NULL END as APPN_DT,
                CASE WHEN SUBMISSION_METHOD_CD IN ('D', 'OS', 'SP') THEN A.CRDATEI ELSE NULL END as APPN_DT,
                ASSESSMENT_DUE_DT                                                                as ASST_DUE_DT,
                NULL                                                                             as APPN_STTS_NO,
                SSP_STG_CODE.CODE_ID                                                             AS APPN_STG_CD,
                ASSESSMENT_OUTCOME_CD                                                            as APPN_STTS_CD,
                EX.ORG_UNIT_CD                                                                   as OWNING_ORG_UNIT_CD,
                s.VERS,
                s.CRUSER,
                s.CRDATEI,
                s.CRTIMEI,
                s.CRTERM,
                s.CRWINDOW,
                s.LAST_MOD_USER,
                s.LAST_MOD_DATEI,
                s.LAST_MOD_TIMEI,
                s.LAST_MOD_TERM,
                s.LAST_MOD_WINDOW,
                APPN_PREF_NO::VARCHAR(20)                                                        as APPN_PREF_NO,
                PREFERENCE_NO::varchar(10)                                                       as OFFER_PREF_NO,
                INCOMPLETE_FG                                                                    as INCOMP_APPN_FG,
                NULL                                                                             as TECHONE_FLD1,
                NULL                                                                             as TECHONE_FLD2,
                NULL                                                                             as TECHONE_FLD3,
                NULL                                                                             as TECHONE_FLD4,
                NULL                                                                             as TECHONE_FLD5,
                NULL                                                                             as TECHONE_FLD6,
                NULL                                                                             as TECHONE_FLD7,
                NULL                                                                             as TECHONE_FLD8,
                s.SSP_NO                                                                         as Parent_SSP_NO,
                s.GV1_EXCLUDE_FG,
                GV1_APPLICATION_ID                                                               as GV1_APP_ID,
                GV1_REFERENCE_DT                                                                 as GV1_REF_DT,
                UPFRONT_PAYMENT_AMT                                                              as UPFRONT_PAYMNT_AMT,
                a.UPFRONT_TAX_AMT,
                NULL                                                                             as UPFRONT_PAYMNT_ACK_FG,
                UPFRONT_PAYMNT_CALC_DT                                                           as UPFRONT_PAYMNT_CALC_DATE,
                0                                                                                as UPFRONT_PAYMNT_CALC_TIME,
                UPFRONT_PAYMENT_ACK_DT                                                           as UPFRONT_PAYMNT_ACK_DATE,
                0                                                                                as UPFRONT_PAYMNT_ACK_TIME,
                UPFRONT_WEB_PAYMENT_TYPE                                                         as UPFRONT_WEB_PAYMNT_TYPE,
                UPFRONT_PAYMENT_REF_NO                                                           as UPFRONT_PAYMNT_REF_NO,
                a.UPFRONT_BANK_REF_NO                                                            as UPFRONT_BANK_REF_NO,
                UPFRONT_CONFIRMATION_NO                                                          as UPFRONT_CONFIRM_NO,
                NULL                                                                             as EAP_APPN_SUBM_DT,
                NULL                                                                             as EAP_APPN_SUBM_TM,
                s.APPLICATION_ID,
                s.APPLICATION_LINE_ID,
                o.OFFER_ROUND,
                s.STRUCTURE_ID,
                'N'                                                                              as IS_DELETED,
                SUBMISSION_DT,
                SUBMISSION_METHOD_CD,
                SUB_CD.CODE_DESCR                                                                as SUBMISSION_METHOD
         FROM ODS.AMIS.S1APP_APPLICATION as a
                  INNER JOIN ODS.AMIS.S1APP_APPLICATION_LINE as l on a.application_id = l.application_id

                  INNER JOIN ODS.AMIS.S1APP_STUDY as s on l.application_id = s.application_id
             and s.application_line_id = l.application_line_id
                  LEFT JOIN ODS.AMIS.S1APP_OFFER as o on o.application_id = l.application_id
             and l.application_line_id = o.application_line_id
                  LEFT JOIN ODS.AMIS.S1STU_APPLICATION as x
                            on x.application_id = l.application_id and x.application_line_id = l.application_line_id
                  LEFT JOIN ODS.AMIS.S1SSP_STU_SPK AS P ON P.STU_ID = s.STU_ID
             AND P.SSP_NO = s.SSP_NO
             AND P.SPK_NO = s.SPK_NO
             AND P.SPK_VER_NO = s.SPK_VER_NO
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SSP_STG_CODE ON SSP_STG_CODE.CODE_ID = P.SSP_STG_CD
             AND SSP_STG_CODE.CODE_TYPE = 'SSP_STG_CD'
                  LEFT JOIN ODS.AMIS.S1STC_CODE as SUB_CD ON SUB_CD.CODE_ID = SUBMISSION_METHOD_CD
             AND SUB_CD.CODE_TYPE = 'SUBMISSION_METHOD_CD'
                  LEFT JOIN

              (
                  SELECT SPK_NO,
                         SPK_VER_NO,
                         ORG_UNIT_CD,
                         AVAIL_KEY_NO,
                         ROW_NUMBER() OVER (PARTITION BY SPK_NO, SPK_VER_NO, AVAIL_KEY_NO ORDER BY AVAIL_KEY_NO DESC) AS RN
                  FROM ODS.AMIS.S1SPK_AVAIL_ORG
              ) AS EX ON EX.SPK_NO = S.SPK_NO and EX.SPK_VER_NO = S.SPK_VER_NO
                  AND EX.AVAIL_KEY_NO = S.AVAIL_KEY_NO AND EX.RN = 1

                  LEFT JOIN (
             SELECT ORG_UNIT_CD,
                    ORG_UNIT_NM,
                    ORG_UNIT_SHORT_NM,
                    ORG_UNIT_TYPE_CD,
                    ROW_NUMBER() OVER (PARTITION BY ORG_UNIT_CD ORDER BY EFFCT_DT DESC) RN
             FROM ODS.AMIS.S1ORG_UNIT
         ) ORG_UNIT
                            ON ORG_UNIT.ORG_UNIT_CD = EX.ORG_UNIT_CD AND ORG_UNIT.RN = 1
     ) as A
WHERE NOT EXISTS(
        SELECT NULL
        FROM (
                 SELECT HUB_COURSE_APPLICATION_KEY,
                        HASH_MD5,
                        LOAD_DTS,
                        LEAD(LOAD_DTS)
                             OVER (PARTITION BY HUB_COURSE_APPLICATION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
                 FROM DATA_VAULT.CORE.SAT_COURSE_APPLICATION
             ) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_COURSE_APPLICATION_KEY = A.HUB_COURSE_APPLICATION_KEY
          AND S.HASH_MD5 = A.HASH_MD5
    );