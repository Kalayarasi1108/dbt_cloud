INSERT INTO DATA_VAULT.CORE.SAT_STUDENT_UNIT_FEE (SAT_STUDENT_UNIT_FEE_SK, HUB_STUDENT_UNIT_FEE_KEY, SOURCE, LOAD_DTS,
                                                  ETL_JOB_ID, HASH_MD5, FEE_SEQ_NO, RECUR_FROM_SEQ_NO, TRGRS_FEE_SEQ_NO,
                                                  SPLIT_SEQ_NO, STU_ID, FEE_LIAB_NO, FEE_YR, SEQ_NO, SSP_NO, SPK_NO,
                                                  SPK_VER_NO, SSP_ATT_NO, AVAIL_YR, SPRD_CD, FEE_EFFCT_DT,
                                                  STU_FEE_START_DT, STU_FEE_END_DT, NEXT_OCCURS_DT, GV1_RPT_REF_YR,
                                                  CENSUS_DT_CD, SUBM_CD, GV1_FEE_AMT, GV1_ALLOC_AMT, ORIG_FEE_AMT,
                                                  ORIG_FEE_DUE_DT, FEE_AMT, FEE_DUE_DT, FIRST_DISC_AMT, FIRST_DISC_DT,
                                                  FEE_TXN_NO, FEE_OVR_TYPE, FEE_OVR_USER_ID, FEE_OVR_REAS, FEE_OVR_AMT,
                                                  FEE_OVR_DUE_DT, FEE_OVR_EFFCT_DT, FEE_OVR_EXP_DT, FEE_OVR_DT_CALC_NM,
                                                  SSP_FEE_MOD_REAS1, SSP_FEE_MOD_REAS2, SSP_MOD_USER_ID,
                                                  SSP_FEE_DBT_STTS, SSP_FEE_DBT_REAS, SSP_FEE_DBT_EFFCT,
                                                  SSP_FEE_DBT_EXPY, NEXT_SPNSR_NO, STU_FEE_NOTES, GV1_FEE_TAX,
                                                  GV1_ALLOC_TAX, ORIG_RATE_TYPE, ORIG_RATE_CODE, ORIG_TAX_RATE,
                                                  ORIG_FEE_TAX, FEE_RATE_TYPE, FEE_RATE_CODE, FEE_TAX_RATE, FEE_TAX,
                                                  FIRST_DISC_TAX, FEE_OVR_RATE_TYPE, FEE_OVR_RATE_CODE,
                                                  FEE_OVR_TAX_RATE, FEE_OVR_TAX, TECHONE_FLD1, TECHONE_FLD3,
                                                  TECHONE_FLD5, TECHONE_FLD6, VERS, CRUSER, CRDATEI, CRTIMEI, CRTERM,
                                                  CRWINDOW, LAST_MOD_USER, LAST_MOD_DATEI, LAST_MOD_TIMEI,
                                                  LAST_MOD_TERM, LAST_MOD_WINDOW, CALDR_YR, PIP_FG, GV1_SEM_CD,
                                                  GOVT_LOAN_SCHM_CD, EP_YEAR, EP_NO, RECALC_CUT_OFF_DT, FEE_REV_TYPE_CD,
                                                  PSPK_LIAB_CAT_CD, STU_CITIZEN_CD, COHORT_YR, PREV_GOVT_FND_SCHM,
                                                  SPLIT_PCENT, HALF_YR_PRD_YR, HALF_YR_PRD_DT_CD, TECHONE_FLD2,
                                                  TECHONE_FLD4, TECHONE_FLD7, TECHONE_FLD8, SA_FEE_INCURRAL_DT,
                                                  DEFERRAL_FG, DEFERRAL_DT, GOVT_REPORT_FG, GOVT_REPORT_DT,
                                                  FORMULA_CALC_VARIABLE, FORMULA_CALC, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_STUDENT_UNIT_FEE_SK,
       MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
           IFNULL(STU_FEES.EP_NO, 0) || ',' ||
           IFNULL(STU_FEES.FEE_SEQ_NO, 0)
           )                                     HUB_STUDENT_UNIT_FEE_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(STU_FEES.FEE_SEQ_NO, 0) || ',' ||
           IFNULL(STU_FEES.RECUR_FROM_SEQ_NO, 0) || ',' ||
           IFNULL(STU_FEES.TRGRS_FEE_SEQ_NO, 0) || ',' ||
           IFNULL(STU_FEES.SPLIT_SEQ_NO, 0) || ',' ||
           IFNULL(STU_FEES.STU_ID, '') || ',' ||
           IFNULL(STU_FEES.FEE_LIAB_NO, 0) || ',' ||
           IFNULL(STU_FEES.FEE_YR, 0) || ',' ||
           IFNULL(STU_FEES.SEQ_NO, 0) || ',' ||
           IFNULL(STU_FEES.SSP_NO, 0) || ',' ||
           IFNULL(STU_FEES.SPK_NO, 0) || ',' ||
           IFNULL(STU_FEES.SPK_VER_NO, 0) || ',' ||
           IFNULL(STU_FEES.SSP_ATT_NO, 0) || ',' ||
           IFNULL(STU_FEES.AVAIL_YR, 0) || ',' ||
           IFNULL(STU_FEES.SPRD_CD, '') || ',' ||
           IFNULL(STU_FEES.FEE_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.STU_FEE_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.STU_FEE_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.NEXT_OCCURS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.GV1_RPT_REF_YR, 0) || ',' ||
           IFNULL(STU_FEES.CENSUS_DT_CD, '') || ',' ||
           IFNULL(STU_FEES.SUBM_CD, '') || ',' ||
           IFNULL(STU_FEES.GV1_FEE_AMT, 0) || ',' ||
           IFNULL(STU_FEES.GV1_ALLOC_AMT, 0) || ',' ||
           IFNULL(STU_FEES.ORIG_FEE_AMT, 0) || ',' ||
           IFNULL(STU_FEES.ORIG_FEE_DUE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.FEE_AMT, 0) || ',' ||
           IFNULL(STU_FEES.FEE_DUE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.FIRST_DISC_AMT, 0) || ',' ||
           IFNULL(STU_FEES.FIRST_DISC_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.FEE_TXN_NO, 0) || ',' ||
           IFNULL(STU_FEES.FEE_OVR_TYPE, '') || ',' ||
           IFNULL(STU_FEES.FEE_OVR_USER_ID, '') || ',' ||
           IFNULL(STU_FEES.FEE_OVR_REAS, '') || ',' ||
           IFNULL(STU_FEES.FEE_OVR_AMT, 0) || ',' ||
           IFNULL(STU_FEES.FEE_OVR_DUE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.FEE_OVR_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.FEE_OVR_EXP_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.FEE_OVR_DT_CALC_NM, '') || ',' ||
           IFNULL(STU_FEES.SSP_FEE_MOD_REAS1, '') || ',' ||
           IFNULL(STU_FEES.SSP_FEE_MOD_REAS2, '') || ',' ||
           IFNULL(STU_FEES.SSP_MOD_USER_ID, '') || ',' ||
           IFNULL(STU_FEES.SSP_FEE_DBT_STTS, '') || ',' ||
           IFNULL(STU_FEES.SSP_FEE_DBT_REAS, '') || ',' ||
           IFNULL(STU_FEES.SSP_FEE_DBT_EFFCT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.SSP_FEE_DBT_EXPY, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.NEXT_SPNSR_NO, 0) || ',' ||
           IFNULL(STU_FEES.STU_FEE_NOTES, '') || ',' ||
           IFNULL(STU_FEES.GV1_FEE_TAX, 0) || ',' ||
           IFNULL(STU_FEES.GV1_ALLOC_TAX, 0) || ',' ||
           IFNULL(STU_FEES.ORIG_RATE_TYPE, '') || ',' ||
           IFNULL(STU_FEES.ORIG_RATE_CODE, '') || ',' ||
           IFNULL(STU_FEES.ORIG_TAX_RATE, 0) || ',' ||
           IFNULL(STU_FEES.ORIG_FEE_TAX, 0) || ',' ||
           IFNULL(STU_FEES.FEE_RATE_TYPE, '') || ',' ||
           IFNULL(STU_FEES.FEE_RATE_CODE, '') || ',' ||
           IFNULL(STU_FEES.FEE_TAX_RATE, 0) || ',' ||
           IFNULL(STU_FEES.FEE_TAX, 0) || ',' ||
           IFNULL(STU_FEES.FIRST_DISC_TAX, 0) || ',' ||
           IFNULL(STU_FEES.FEE_OVR_RATE_TYPE, '') || ',' ||
           IFNULL(STU_FEES.FEE_OVR_RATE_CODE, '') || ',' ||
           IFNULL(STU_FEES.FEE_OVR_TAX_RATE, 0) || ',' ||
           IFNULL(STU_FEES.FEE_OVR_TAX, 0) || ',' ||
           IFNULL(STU_FEES.TECHONE_FLD1, '') || ',' ||
           IFNULL(STU_FEES.TECHONE_FLD3, '') || ',' ||
           IFNULL(STU_FEES.TECHONE_FLD5, 0) || ',' ||
           IFNULL(STU_FEES.TECHONE_FLD6, 0) || ',' ||
           IFNULL(STU_FEES.VERS, 0) || ',' ||
           IFNULL(STU_FEES.CRUSER, '') || ',' ||
           IFNULL(STU_FEES.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.CRTIMEI, 0) || ',' ||
           IFNULL(STU_FEES.CRTERM, '') || ',' ||
           IFNULL(STU_FEES.CRWINDOW, '') || ',' ||
           IFNULL(STU_FEES.LAST_MOD_USER, '') || ',' ||
           IFNULL(STU_FEES.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.LAST_MOD_TIMEI, 0) || ',' ||
           IFNULL(STU_FEES.LAST_MOD_TERM, '') || ',' ||
           IFNULL(STU_FEES.LAST_MOD_WINDOW, '') || ',' ||
           IFNULL(STU_FEES.CALDR_YR, 0) || ',' ||
           IFNULL(STU_FEES.PIP_FG, '') || ',' ||
           IFNULL(STU_FEES.GV1_SEM_CD, '') || ',' ||
           IFNULL(STU_FEES.GOVT_LOAN_SCHM_CD, '') || ',' ||
           IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
           IFNULL(STU_FEES.EP_NO, 0) || ',' ||
           IFNULL(STU_FEES.RECALC_CUT_OFF_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.FEE_REV_TYPE_CD, '') || ',' ||
           IFNULL(STU_FEES.PSPK_LIAB_CAT_CD, '') || ',' ||
           IFNULL(STU_FEES.STU_CITIZEN_CD, '') || ',' ||
           IFNULL(STU_FEES.COHORT_YR, 0) || ',' ||
           IFNULL(STU_FEES.PREV_GOVT_FND_SCHM, '') || ',' ||
           IFNULL(STU_FEES.SPLIT_PCENT, 0) || ',' ||
           IFNULL(STU_FEES.HALF_YR_PRD_YR, 0) || ',' ||
           IFNULL(STU_FEES.HALF_YR_PRD_DT_CD, '') || ',' ||
           IFNULL(STU_FEES.TECHONE_FLD2, '') || ',' ||
           IFNULL(STU_FEES.TECHONE_FLD4, '') || ',' ||
           IFNULL(STU_FEES.TECHONE_FLD7, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.TECHONE_FLD8, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.SA_FEE_INCURRAL_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.DEFERRAL_FG, '') || ',' ||
           IFNULL(STU_FEES.DEFERRAL_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.GOVT_REPORT_FG, '') || ',' ||
           IFNULL(STU_FEES.GOVT_REPORT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_FEES.FORMULA_CALC_VARIABLE, '') || ',' ||
           IFNULL(STU_FEES.FORMULA_CALC, '') || ',' ||
           'N'
           )                                     HASH_MD5,
       STU_FEES.FEE_SEQ_NO,
       STU_FEES.RECUR_FROM_SEQ_NO,
       STU_FEES.TRGRS_FEE_SEQ_NO,
       STU_FEES.SPLIT_SEQ_NO,
       STU_FEES.STU_ID,
       STU_FEES.FEE_LIAB_NO,
       STU_FEES.FEE_YR,
       STU_FEES.SEQ_NO,
       STU_FEES.SSP_NO,
       STU_FEES.SPK_NO,
       STU_FEES.SPK_VER_NO,
       STU_FEES.SSP_ATT_NO,
       STU_FEES.AVAIL_YR,
       STU_FEES.SPRD_CD,
       STU_FEES.FEE_EFFCT_DT,
       STU_FEES.STU_FEE_START_DT,
       STU_FEES.STU_FEE_END_DT,
       STU_FEES.NEXT_OCCURS_DT,
       STU_FEES.GV1_RPT_REF_YR,
       STU_FEES.CENSUS_DT_CD,
       STU_FEES.SUBM_CD,
       STU_FEES.GV1_FEE_AMT,
       STU_FEES.GV1_ALLOC_AMT,
       STU_FEES.ORIG_FEE_AMT,
       STU_FEES.ORIG_FEE_DUE_DT,
       STU_FEES.FEE_AMT,
       STU_FEES.FEE_DUE_DT,
       STU_FEES.FIRST_DISC_AMT,
       STU_FEES.FIRST_DISC_DT,
       STU_FEES.FEE_TXN_NO,
       STU_FEES.FEE_OVR_TYPE,
       STU_FEES.FEE_OVR_USER_ID,
       STU_FEES.FEE_OVR_REAS,
       STU_FEES.FEE_OVR_AMT,
       STU_FEES.FEE_OVR_DUE_DT,
       STU_FEES.FEE_OVR_EFFCT_DT,
       STU_FEES.FEE_OVR_EXP_DT,
       STU_FEES.FEE_OVR_DT_CALC_NM,
       STU_FEES.SSP_FEE_MOD_REAS1,
       STU_FEES.SSP_FEE_MOD_REAS2,
       STU_FEES.SSP_MOD_USER_ID,
       STU_FEES.SSP_FEE_DBT_STTS,
       STU_FEES.SSP_FEE_DBT_REAS,
       STU_FEES.SSP_FEE_DBT_EFFCT,
       STU_FEES.SSP_FEE_DBT_EXPY,
       STU_FEES.NEXT_SPNSR_NO,
       STU_FEES.STU_FEE_NOTES,
       STU_FEES.GV1_FEE_TAX,
       STU_FEES.GV1_ALLOC_TAX,
       STU_FEES.ORIG_RATE_TYPE,
       STU_FEES.ORIG_RATE_CODE,
       STU_FEES.ORIG_TAX_RATE,
       STU_FEES.ORIG_FEE_TAX,
       STU_FEES.FEE_RATE_TYPE,
       STU_FEES.FEE_RATE_CODE,
       STU_FEES.FEE_TAX_RATE,
       STU_FEES.FEE_TAX,
       STU_FEES.FIRST_DISC_TAX,
       STU_FEES.FEE_OVR_RATE_TYPE,
       STU_FEES.FEE_OVR_RATE_CODE,
       STU_FEES.FEE_OVR_TAX_RATE,
       STU_FEES.FEE_OVR_TAX,
       STU_FEES.TECHONE_FLD1,
       STU_FEES.TECHONE_FLD3,
       STU_FEES.TECHONE_FLD5,
       STU_FEES.TECHONE_FLD6,
       STU_FEES.VERS,
       STU_FEES.CRUSER,
       STU_FEES.CRDATEI,
       STU_FEES.CRTIMEI,
       STU_FEES.CRTERM,
       STU_FEES.CRWINDOW,
       STU_FEES.LAST_MOD_USER,
       STU_FEES.LAST_MOD_DATEI,
       STU_FEES.LAST_MOD_TIMEI,
       STU_FEES.LAST_MOD_TERM,
       STU_FEES.LAST_MOD_WINDOW,
       STU_FEES.CALDR_YR,
       STU_FEES.PIP_FG,
       STU_FEES.GV1_SEM_CD,
       STU_FEES.GOVT_LOAN_SCHM_CD,
       STU_FEES.EP_YEAR,
       STU_FEES.EP_NO,
       STU_FEES.RECALC_CUT_OFF_DT,
       STU_FEES.FEE_REV_TYPE_CD,
       STU_FEES.PSPK_LIAB_CAT_CD,
       STU_FEES.STU_CITIZEN_CD,
       STU_FEES.COHORT_YR,
       STU_FEES.PREV_GOVT_FND_SCHM,
       STU_FEES.SPLIT_PCENT,
       STU_FEES.HALF_YR_PRD_YR,
       STU_FEES.HALF_YR_PRD_DT_CD,
       STU_FEES.TECHONE_FLD2,
       STU_FEES.TECHONE_FLD4,
       STU_FEES.TECHONE_FLD7,
       STU_FEES.TECHONE_FLD8,
       STU_FEES.SA_FEE_INCURRAL_DT,
       STU_FEES.DEFERRAL_FG,
       STU_FEES.DEFERRAL_DT,
       STU_FEES.GOVT_REPORT_FG,
       STU_FEES.GOVT_REPORT_DT,
       STU_FEES.FORMULA_CALC_VARIABLE,
       STU_FEES.FORMULA_CALC,
       'N'                                       IS_DELETED
FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
         JOIN ODS.AMIS.S1STU_FEES STU_FEES
              ON STU_FEES.SSP_NO = UN_SSP.SSP_NO
WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_STUDENT_UNIT_FEE_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_STUDENT_UNIT_FEE_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_STUDENT_UNIT_FEE) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_STUDENT_UNIT_FEE_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                               IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                                               IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
                                               IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
                                               IFNULL(STU_FEES.EP_NO, 0) || ',' ||
                                               IFNULL(STU_FEES.FEE_SEQ_NO, 0)
            )
          AND S.HASH_MD5 = MD5(IFNULL(STU_FEES.FEE_SEQ_NO, 0) || ',' ||
                               IFNULL(STU_FEES.RECUR_FROM_SEQ_NO, 0) || ',' ||
                               IFNULL(STU_FEES.TRGRS_FEE_SEQ_NO, 0) || ',' ||
                               IFNULL(STU_FEES.SPLIT_SEQ_NO, 0) || ',' ||
                               IFNULL(STU_FEES.STU_ID, '') || ',' ||
                               IFNULL(STU_FEES.FEE_LIAB_NO, 0) || ',' ||
                               IFNULL(STU_FEES.FEE_YR, 0) || ',' ||
                               IFNULL(STU_FEES.SEQ_NO, 0) || ',' ||
                               IFNULL(STU_FEES.SSP_NO, 0) || ',' ||
                               IFNULL(STU_FEES.SPK_NO, 0) || ',' ||
                               IFNULL(STU_FEES.SPK_VER_NO, 0) || ',' ||
                               IFNULL(STU_FEES.SSP_ATT_NO, 0) || ',' ||
                               IFNULL(STU_FEES.AVAIL_YR, 0) || ',' ||
                               IFNULL(STU_FEES.SPRD_CD, '') || ',' ||
                               IFNULL(STU_FEES.FEE_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.STU_FEE_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.STU_FEE_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.NEXT_OCCURS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.GV1_RPT_REF_YR, 0) || ',' ||
                               IFNULL(STU_FEES.CENSUS_DT_CD, '') || ',' ||
                               IFNULL(STU_FEES.SUBM_CD, '') || ',' ||
                               IFNULL(STU_FEES.GV1_FEE_AMT, 0) || ',' ||
                               IFNULL(STU_FEES.GV1_ALLOC_AMT, 0) || ',' ||
                               IFNULL(STU_FEES.ORIG_FEE_AMT, 0) || ',' ||
                               IFNULL(STU_FEES.ORIG_FEE_DUE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.FEE_AMT, 0) || ',' ||
                               IFNULL(STU_FEES.FEE_DUE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.FIRST_DISC_AMT, 0) || ',' ||
                               IFNULL(STU_FEES.FIRST_DISC_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.FEE_TXN_NO, 0) || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_TYPE, '') || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_USER_ID, '') || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_REAS, '') || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_AMT, 0) || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_DUE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_EXP_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_DT_CALC_NM, '') || ',' ||
                               IFNULL(STU_FEES.SSP_FEE_MOD_REAS1, '') || ',' ||
                               IFNULL(STU_FEES.SSP_FEE_MOD_REAS2, '') || ',' ||
                               IFNULL(STU_FEES.SSP_MOD_USER_ID, '') || ',' ||
                               IFNULL(STU_FEES.SSP_FEE_DBT_STTS, '') || ',' ||
                               IFNULL(STU_FEES.SSP_FEE_DBT_REAS, '') || ',' ||
                               IFNULL(STU_FEES.SSP_FEE_DBT_EFFCT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.SSP_FEE_DBT_EXPY, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.NEXT_SPNSR_NO, 0) || ',' ||
                               IFNULL(STU_FEES.STU_FEE_NOTES, '') || ',' ||
                               IFNULL(STU_FEES.GV1_FEE_TAX, 0) || ',' ||
                               IFNULL(STU_FEES.GV1_ALLOC_TAX, 0) || ',' ||
                               IFNULL(STU_FEES.ORIG_RATE_TYPE, '') || ',' ||
                               IFNULL(STU_FEES.ORIG_RATE_CODE, '') || ',' ||
                               IFNULL(STU_FEES.ORIG_TAX_RATE, 0) || ',' ||
                               IFNULL(STU_FEES.ORIG_FEE_TAX, 0) || ',' ||
                               IFNULL(STU_FEES.FEE_RATE_TYPE, '') || ',' ||
                               IFNULL(STU_FEES.FEE_RATE_CODE, '') || ',' ||
                               IFNULL(STU_FEES.FEE_TAX_RATE, 0) || ',' ||
                               IFNULL(STU_FEES.FEE_TAX, 0) || ',' ||
                               IFNULL(STU_FEES.FIRST_DISC_TAX, 0) || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_RATE_TYPE, '') || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_RATE_CODE, '') || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_TAX_RATE, 0) || ',' ||
                               IFNULL(STU_FEES.FEE_OVR_TAX, 0) || ',' ||
                               IFNULL(STU_FEES.TECHONE_FLD1, '') || ',' ||
                               IFNULL(STU_FEES.TECHONE_FLD3, '') || ',' ||
                               IFNULL(STU_FEES.TECHONE_FLD5, 0) || ',' ||
                               IFNULL(STU_FEES.TECHONE_FLD6, 0) || ',' ||
                               IFNULL(STU_FEES.VERS, 0) || ',' ||
                               IFNULL(STU_FEES.CRUSER, '') || ',' ||
                               IFNULL(STU_FEES.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.CRTIMEI, 0) || ',' ||
                               IFNULL(STU_FEES.CRTERM, '') || ',' ||
                               IFNULL(STU_FEES.CRWINDOW, '') || ',' ||
                               IFNULL(STU_FEES.LAST_MOD_USER, '') || ',' ||
                               IFNULL(STU_FEES.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.LAST_MOD_TIMEI, 0) || ',' ||
                               IFNULL(STU_FEES.LAST_MOD_TERM, '') || ',' ||
                               IFNULL(STU_FEES.LAST_MOD_WINDOW, '') || ',' ||
                               IFNULL(STU_FEES.CALDR_YR, 0) || ',' ||
                               IFNULL(STU_FEES.PIP_FG, '') || ',' ||
                               IFNULL(STU_FEES.GV1_SEM_CD, '') || ',' ||
                               IFNULL(STU_FEES.GOVT_LOAN_SCHM_CD, '') || ',' ||
                               IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
                               IFNULL(STU_FEES.EP_NO, 0) || ',' ||
                               IFNULL(STU_FEES.RECALC_CUT_OFF_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.FEE_REV_TYPE_CD, '') || ',' ||
                               IFNULL(STU_FEES.PSPK_LIAB_CAT_CD, '') || ',' ||
                               IFNULL(STU_FEES.STU_CITIZEN_CD, '') || ',' ||
                               IFNULL(STU_FEES.COHORT_YR, 0) || ',' ||
                               IFNULL(STU_FEES.PREV_GOVT_FND_SCHM, '') || ',' ||
                               IFNULL(STU_FEES.SPLIT_PCENT, 0) || ',' ||
                               IFNULL(STU_FEES.HALF_YR_PRD_YR, 0) || ',' ||
                               IFNULL(STU_FEES.HALF_YR_PRD_DT_CD, '') || ',' ||
                               IFNULL(STU_FEES.TECHONE_FLD2, '') || ',' ||
                               IFNULL(STU_FEES.TECHONE_FLD4, '') || ',' ||
                               IFNULL(STU_FEES.TECHONE_FLD7, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.TECHONE_FLD8, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.SA_FEE_INCURRAL_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.DEFERRAL_FG, '') || ',' ||
                               IFNULL(STU_FEES.DEFERRAL_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.GOVT_REPORT_FG, '') || ',' ||
                               IFNULL(STU_FEES.GOVT_REPORT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_FEES.FORMULA_CALC_VARIABLE, '') || ',' ||
                               IFNULL(STU_FEES.FORMULA_CALC, '') || ',' ||
                               'N'
            )
    )
;