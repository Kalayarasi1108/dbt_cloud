INSERT INTO DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_EP (SAT_UNIT_ENROLMENT_EP_SK, HUB_UNIT_ENROLMENT_EP_KEY, SOURCE,
                                                   LOAD_DTS, ETL_JOB_ID, HASH_MD5, SSP_NO, EP_YEAR, EP_NO, LOCATION_CD,
                                                   EP_TYPE_CD, EP_FREQ_CD, EP_CYR_NO, EP_CYR_RD_FG, EP_START_DT,
                                                   EP_END_DT, EP_YEAR_PCENT, EP_CYR_CTL_CENS_DT, CTL_CENSUS_DT,
                                                   EP_FIRST_NZERO_DT, EP_LAST_NZERO_DT, GV1_STD_CD, GV1_YR, GV1_SEM_CD,
                                                   GV1_CENSUS_DT_CD, GROSS_NORM_EFTSU, GROSS_FEC_EFTSU, SUBS_NORM_EFTSU,
                                                   SUBS_FEC_EFTSU, NETT_NORM_EFTSU, NETT_FEC_EFTSU, LIAB_CAT_CD,
                                                   FEC_LIAB_CAT_CD, LOAD_CAT_CD, ATTNDC_MODE_CD, STUDY_MODE_CD, FOS_CD,
                                                   FOE_CD, DISCPLN_GRP_CD, FLD_RESCH_CD, SOCIO_EC_CD, FEES_TRGR_FG,
                                                   TECHONE_FLD1, TECHONE_FLD2, TECHONE_FLD3, TECHONE_FLD4, TECHONE_FLD5,
                                                   TECHONE_FLD6, VERS, CRUSER, CRDATEI, CRTIMEI, CRTERM, CRWINDOW,
                                                   LAST_MOD_USER, LAST_MOD_DATEI, LAST_MOD_TIMEI, LAST_MOD_TERM,
                                                   LAST_MOD_WINDOW, STU_STTS_CD, STU_STTS_ASSIGN_FG, GOVT_REPORT_DT,
                                                   GOVT_REPORT_IND, STU_STTS_MOD_DATEI, STU_STTS_MOD_USER,
                                                   STU_STTS_MOD_WIN, CENSUS_DT_IND, DFRL_IND, DFRL_DT, VAR_IND,
                                                   GOVT_REPORT_HIST_IND, LAST_RPTBL_RVSN_NO, RMTD_IND, RMTD_DT,
                                                   GV1_COMPONENT_SSP_NO, GV1_PARENT_SSP_NO, GV1_PARENT_CRS_CD,
                                                   GV1_SPLIT_NORM_FEC_IND, FEC_CENSUS_DT, FEC_STU_STTS_CD,
                                                   FEC_STU_STTS_MOD_DT, TECHONE_FLD7, TECHONE_FLD8,
                                                   LIABILITY_CATEGORY,
                                                   LOAD_CATEGORY,
                                                   ATTENDANCE_MODE,
                                                   STUDY_MODE,
                                                   IS_DELETED,
                                                   COMMENCE_OR_CONTINUE)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_UNIT_ENROLMENT_EP_SK,
       MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(SSP_EP_DTL.EP_YEAR, 0) || ',' ||
           IFNULL(SSP_EP_DTL.EP_NO, 0)
           )                                     HUB_UNIT_ENROLMENT_EP_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(
                   IFNULL(SSP_EP_DTL.SSP_NO, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.EP_YEAR, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.EP_NO, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.LOCATION_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.EP_TYPE_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.EP_FREQ_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.EP_CYR_NO, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.EP_CYR_RD_FG, '') || ',' ||
                   IFNULL(SSP_EP_DTL.EP_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.EP_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.EP_YEAR_PCENT, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.EP_CYR_CTL_CENS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.CTL_CENSUS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.EP_FIRST_NZERO_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.EP_LAST_NZERO_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.GV1_STD_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.GV1_YR, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.GV1_SEM_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.GV1_CENSUS_DT_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.GROSS_NORM_EFTSU, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.GROSS_FEC_EFTSU, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.SUBS_NORM_EFTSU, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.SUBS_FEC_EFTSU, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.NETT_NORM_EFTSU, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.NETT_FEC_EFTSU, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.LIAB_CAT_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.FEC_LIAB_CAT_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.LOAD_CAT_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.ATTNDC_MODE_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.STUDY_MODE_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.FOS_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.FOE_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.DISCPLN_GRP_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.FLD_RESCH_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.SOCIO_EC_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.FEES_TRGR_FG, '') || ',' ||
                   IFNULL(SSP_EP_DTL.TECHONE_FLD1, '') || ',' ||
                   IFNULL(SSP_EP_DTL.TECHONE_FLD2, '') || ',' ||
                   IFNULL(SSP_EP_DTL.TECHONE_FLD3, '') || ',' ||
                   IFNULL(SSP_EP_DTL.TECHONE_FLD4, '') || ',' ||
                   IFNULL(SSP_EP_DTL.TECHONE_FLD5, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.TECHONE_FLD6, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.VERS, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.CRUSER, '') || ',' ||
                   IFNULL(SSP_EP_DTL.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.CRTIMEI, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.CRTERM, '') || ',' ||
                   IFNULL(SSP_EP_DTL.CRWINDOW, '') || ',' ||
                   IFNULL(SSP_EP_DTL.LAST_MOD_USER, '') || ',' ||
                   IFNULL(SSP_EP_DTL.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.LAST_MOD_TIMEI, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.LAST_MOD_TERM, '') || ',' ||
                   IFNULL(SSP_EP_DTL.LAST_MOD_WINDOW, '') || ',' ||
                   IFNULL(SSP_EP_DTL.STU_STTS_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.STU_STTS_ASSIGN_FG, '') || ',' ||
                   IFNULL(SSP_EP_DTL.GOVT_REPORT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.GOVT_REPORT_IND, '') || ',' ||
                   IFNULL(SSP_EP_DTL.STU_STTS_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.STU_STTS_MOD_USER, '') || ',' ||
                   IFNULL(SSP_EP_DTL.STU_STTS_MOD_WIN, '') || ',' ||
                   IFNULL(SSP_EP_DTL.CENSUS_DT_IND, '') || ',' ||
                   IFNULL(SSP_EP_DTL.DFRL_IND, '') || ',' ||
                   IFNULL(SSP_EP_DTL.DFRL_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.VAR_IND, '') || ',' ||
                   IFNULL(SSP_EP_DTL.GOVT_REPORT_HIST_IND, '') || ',' ||
                   IFNULL(SSP_EP_DTL.LAST_RPTBL_RVSN_NO, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.RMTD_IND, '') || ',' ||
                   IFNULL(SSP_EP_DTL.RMTD_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.GV1_COMPONENT_SSP_NO, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.GV1_PARENT_SSP_NO, 0) || ',' ||
                   IFNULL(SSP_EP_DTL.GV1_PARENT_CRS_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.GV1_SPLIT_NORM_FEC_IND, '') || ',' ||
                   IFNULL(SSP_EP_DTL.FEC_CENSUS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.FEC_STU_STTS_CD, '') || ',' ||
                   IFNULL(SSP_EP_DTL.FEC_STU_STTS_MOD_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.TECHONE_FLD7, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_EP_DTL.TECHONE_FLD8, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_LIABILITY_CATEGORY_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(LOAD_CATEGORY_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(ATTENDANCE_MODE_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(STUDY_MODE_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL('N', '') || ',' ||
                   IFNULL(CASE
                              WHEN YEAR(SSP_EP_DTL.CTL_CENSUS_DT) =
                                   YEAR(COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE,
                                                 COURSE_START_DATE.EXPECTED_DATE)) THEN 'Commencing'
                              WHEN YEAR(SSP_EP_DTL.CTL_CENSUS_DT) >
                                   YEAR(COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE,
                                                 COURSE_START_DATE.EXPECTED_DATE)) THEN 'Continuing'
                              ELSE 'Not Applicable'
                              END, '')
           )                                     HASH_MD5,
       SSP_EP_DTL.SSP_NO,
       SSP_EP_DTL.EP_YEAR,
       SSP_EP_DTL.EP_NO,
       SSP_EP_DTL.LOCATION_CD,
       SSP_EP_DTL.EP_TYPE_CD,
       SSP_EP_DTL.EP_FREQ_CD,
       SSP_EP_DTL.EP_CYR_NO,
       SSP_EP_DTL.EP_CYR_RD_FG,
       SSP_EP_DTL.EP_START_DT,
       SSP_EP_DTL.EP_END_DT,
       SSP_EP_DTL.EP_YEAR_PCENT,
       SSP_EP_DTL.EP_CYR_CTL_CENS_DT,
       SSP_EP_DTL.CTL_CENSUS_DT,
       SSP_EP_DTL.EP_FIRST_NZERO_DT,
       SSP_EP_DTL.EP_LAST_NZERO_DT,
       SSP_EP_DTL.GV1_STD_CD,
       SSP_EP_DTL.GV1_YR,
       SSP_EP_DTL.GV1_SEM_CD,
       SSP_EP_DTL.GV1_CENSUS_DT_CD,
       SSP_EP_DTL.GROSS_NORM_EFTSU,
       SSP_EP_DTL.GROSS_FEC_EFTSU,
       SSP_EP_DTL.SUBS_NORM_EFTSU,
       SSP_EP_DTL.SUBS_FEC_EFTSU,
       SSP_EP_DTL.NETT_NORM_EFTSU,
       SSP_EP_DTL.NETT_FEC_EFTSU,
       SSP_EP_DTL.LIAB_CAT_CD,
       SSP_EP_DTL.FEC_LIAB_CAT_CD,
       SSP_EP_DTL.LOAD_CAT_CD,
       SSP_EP_DTL.ATTNDC_MODE_CD,
       SSP_EP_DTL.STUDY_MODE_CD,
       SSP_EP_DTL.FOS_CD,
       SSP_EP_DTL.FOE_CD,
       SSP_EP_DTL.DISCPLN_GRP_CD,
       SSP_EP_DTL.FLD_RESCH_CD,
       SSP_EP_DTL.SOCIO_EC_CD,
       SSP_EP_DTL.FEES_TRGR_FG,
       SSP_EP_DTL.TECHONE_FLD1,
       SSP_EP_DTL.TECHONE_FLD2,
       SSP_EP_DTL.TECHONE_FLD3,
       SSP_EP_DTL.TECHONE_FLD4,
       SSP_EP_DTL.TECHONE_FLD5,
       SSP_EP_DTL.TECHONE_FLD6,
       SSP_EP_DTL.VERS,
       SSP_EP_DTL.CRUSER,
       SSP_EP_DTL.CRDATEI,
       SSP_EP_DTL.CRTIMEI,
       SSP_EP_DTL.CRTERM,
       SSP_EP_DTL.CRWINDOW,
       SSP_EP_DTL.LAST_MOD_USER,
       SSP_EP_DTL.LAST_MOD_DATEI,
       SSP_EP_DTL.LAST_MOD_TIMEI,
       SSP_EP_DTL.LAST_MOD_TERM,
       SSP_EP_DTL.LAST_MOD_WINDOW,
       SSP_EP_DTL.STU_STTS_CD,
       SSP_EP_DTL.STU_STTS_ASSIGN_FG,
       SSP_EP_DTL.GOVT_REPORT_DT,
       SSP_EP_DTL.GOVT_REPORT_IND,
       SSP_EP_DTL.STU_STTS_MOD_DATEI,
       SSP_EP_DTL.STU_STTS_MOD_USER,
       SSP_EP_DTL.STU_STTS_MOD_WIN,
       SSP_EP_DTL.CENSUS_DT_IND,
       SSP_EP_DTL.DFRL_IND,
       SSP_EP_DTL.DFRL_DT,
       SSP_EP_DTL.VAR_IND,
       SSP_EP_DTL.GOVT_REPORT_HIST_IND,
       SSP_EP_DTL.LAST_RPTBL_RVSN_NO,
       SSP_EP_DTL.RMTD_IND,
       SSP_EP_DTL.RMTD_DT,
       SSP_EP_DTL.GV1_COMPONENT_SSP_NO,
       SSP_EP_DTL.GV1_PARENT_SSP_NO,
       SSP_EP_DTL.GV1_PARENT_CRS_CD,
       SSP_EP_DTL.GV1_SPLIT_NORM_FEC_IND,
       SSP_EP_DTL.FEC_CENSUS_DT,
       SSP_EP_DTL.FEC_STU_STTS_CD,
       SSP_EP_DTL.FEC_STU_STTS_MOD_DT,
       SSP_EP_DTL.TECHONE_FLD7,
       SSP_EP_DTL.TECHONE_FLD8,
       UNIT_LIABILITY_CATEGORY_CODE.CODE_DESCR   LIABILITY_CATEGORY,
       LOAD_CATEGORY_CODE.CODE_DESCR             LOAD_CATEGORY,
       ATTENDANCE_MODE_CODE.CODE_DESCR           ATTENDANCE_MODE,
       STUDY_MODE_CODE.CODE_DESCR                STUDY_MODE,
       'N'                                       IS_DELETED,
       CASE
           WHEN YEAR(SSP_EP_DTL.CTL_CENSUS_DT) =
                YEAR(COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE, COURSE_START_DATE.EXPECTED_DATE)) THEN 'Commencing'
           WHEN YEAR(SSP_EP_DTL.CTL_CENSUS_DT) >
                YEAR(COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE, COURSE_START_DATE.EXPECTED_DATE)) THEN 'Continuing'
           ELSE 'Not Applicable'
           END                                   "COMMENCE_OR_CONTINUE"
FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
         JOIN ODS.AMIS.S1SSP_EP_DTL SSP_EP_DTL
              ON SSP_EP_DTL.SSP_NO = UN_SSP.SSP_NO
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_MODE_CODE
                         ON STUDY_MODE_CODE.CODE_ID = SSP_EP_DTL.STUDY_MODE_CD AND
                            STUDY_MODE_CODE.CODE_TYPE = 'STUDY_MODE_CD'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE ATTENDANCE_MODE_CODE
                         ON ATTENDANCE_MODE_CODE.CODE_ID = SSP_EP_DTL.ATTNDC_MODE_CD AND
                            ATTENDANCE_MODE_CODE.CODE_TYPE = 'ATTNDC_MODE_CD'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE LOAD_CATEGORY_CODE
                         ON LOAD_CATEGORY_CODE.CODE_ID = SSP_EP_DTL.LOAD_CAT_CD AND
                            LOAD_CATEGORY_CODE.CODE_TYPE = 'LOAD_CAT_CD'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE UNIT_LIABILITY_CATEGORY_CODE
                         ON UNIT_LIABILITY_CATEGORY_CODE.CODE_ID = SSP_EP_DTL.LIAB_CAT_CD AND
                            UNIT_LIABILITY_CATEGORY_CODE.CODE_TYPE = 'LIAB_CAT_CD'
         LEFT OUTER JOIN ODS.AMIS.S1SSP_DATE COURSE_START_DATE
                         ON COURSE_START_DATE.SSP_NO = UN_SSP.PARENT_SSP_NO and
                            COURSE_START_DATE.SSP_DT_TYPE_CD = 'STRT'
         LEFT OUTER JOIN ODS.AMIS.S1SSP_DATE COURSE_STUDY_START_DATE
                         ON COURSE_STUDY_START_DATE.SSP_NO = UN_SSP.PARENT_SSP_NO AND
                            COURSE_STUDY_START_DATE.SSP_DT_TYPE_CD = 'COS'
WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_UNIT_ENROLMENT_EP_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS)
                          OVER (PARTITION BY HUB_UNIT_ENROLMENT_EP_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_EP) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_UNIT_ENROLMENT_EP_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                                IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                                                IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                                IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                                                IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                                                IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                                                IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                                IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                                                IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
                                                IFNULL(SSP_EP_DTL.EP_YEAR, 0) || ',' ||
                                                IFNULL(SSP_EP_DTL.EP_NO, 0)
            )
          AND S.HASH_MD5 = MD5(
                    IFNULL(SSP_EP_DTL.SSP_NO, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.EP_YEAR, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.EP_NO, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.LOCATION_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.EP_TYPE_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.EP_FREQ_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.EP_CYR_NO, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.EP_CYR_RD_FG, '') || ',' ||
                    IFNULL(SSP_EP_DTL.EP_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.EP_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.EP_YEAR_PCENT, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.EP_CYR_CTL_CENS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.CTL_CENSUS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.EP_FIRST_NZERO_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.EP_LAST_NZERO_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.GV1_STD_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.GV1_YR, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.GV1_SEM_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.GV1_CENSUS_DT_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.GROSS_NORM_EFTSU, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.GROSS_FEC_EFTSU, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.SUBS_NORM_EFTSU, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.SUBS_FEC_EFTSU, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.NETT_NORM_EFTSU, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.NETT_FEC_EFTSU, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.LIAB_CAT_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.FEC_LIAB_CAT_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.LOAD_CAT_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.ATTNDC_MODE_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.STUDY_MODE_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.FOS_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.FOE_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.DISCPLN_GRP_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.FLD_RESCH_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.SOCIO_EC_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.FEES_TRGR_FG, '') || ',' ||
                    IFNULL(SSP_EP_DTL.TECHONE_FLD1, '') || ',' ||
                    IFNULL(SSP_EP_DTL.TECHONE_FLD2, '') || ',' ||
                    IFNULL(SSP_EP_DTL.TECHONE_FLD3, '') || ',' ||
                    IFNULL(SSP_EP_DTL.TECHONE_FLD4, '') || ',' ||
                    IFNULL(SSP_EP_DTL.TECHONE_FLD5, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.TECHONE_FLD6, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.VERS, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.CRUSER, '') || ',' ||
                    IFNULL(SSP_EP_DTL.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.CRTIMEI, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.CRTERM, '') || ',' ||
                    IFNULL(SSP_EP_DTL.CRWINDOW, '') || ',' ||
                    IFNULL(SSP_EP_DTL.LAST_MOD_USER, '') || ',' ||
                    IFNULL(SSP_EP_DTL.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.LAST_MOD_TIMEI, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.LAST_MOD_TERM, '') || ',' ||
                    IFNULL(SSP_EP_DTL.LAST_MOD_WINDOW, '') || ',' ||
                    IFNULL(SSP_EP_DTL.STU_STTS_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.STU_STTS_ASSIGN_FG, '') || ',' ||
                    IFNULL(SSP_EP_DTL.GOVT_REPORT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.GOVT_REPORT_IND, '') || ',' ||
                    IFNULL(SSP_EP_DTL.STU_STTS_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.STU_STTS_MOD_USER, '') || ',' ||
                    IFNULL(SSP_EP_DTL.STU_STTS_MOD_WIN, '') || ',' ||
                    IFNULL(SSP_EP_DTL.CENSUS_DT_IND, '') || ',' ||
                    IFNULL(SSP_EP_DTL.DFRL_IND, '') || ',' ||
                    IFNULL(SSP_EP_DTL.DFRL_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.VAR_IND, '') || ',' ||
                    IFNULL(SSP_EP_DTL.GOVT_REPORT_HIST_IND, '') || ',' ||
                    IFNULL(SSP_EP_DTL.LAST_RPTBL_RVSN_NO, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.RMTD_IND, '') || ',' ||
                    IFNULL(SSP_EP_DTL.RMTD_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.GV1_COMPONENT_SSP_NO, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.GV1_PARENT_SSP_NO, 0) || ',' ||
                    IFNULL(SSP_EP_DTL.GV1_PARENT_CRS_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.GV1_SPLIT_NORM_FEC_IND, '') || ',' ||
                    IFNULL(SSP_EP_DTL.FEC_CENSUS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.FEC_STU_STTS_CD, '') || ',' ||
                    IFNULL(SSP_EP_DTL.FEC_STU_STTS_MOD_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.TECHONE_FLD7, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_EP_DTL.TECHONE_FLD8, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_LIABILITY_CATEGORY_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(LOAD_CATEGORY_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(ATTENDANCE_MODE_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(STUDY_MODE_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL('N', '') || ',' ||
                    IFNULL(CASE
                               WHEN YEAR(SSP_EP_DTL.CTL_CENSUS_DT) =
                                    YEAR(COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE,
                                                  COURSE_START_DATE.EXPECTED_DATE)) THEN 'Commencing'
                               WHEN YEAR(SSP_EP_DTL.CTL_CENSUS_DT) >
                                    YEAR(COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE,
                                                  COURSE_START_DATE.EXPECTED_DATE)) THEN 'Continuing'
                               ELSE 'Not Applicable'
                               END, '')
            )
    )
;