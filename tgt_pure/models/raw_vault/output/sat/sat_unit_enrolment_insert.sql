INSERT INTO DATA_VAULT.CORE.SAT_UNIT_ENROLMENT (SAT_UNIT_ENROLMENT_SK, HUB_UNIT_ENROLMENT_KEY, SOURCE, LOAD_DTS,
                                                ETL_JOB_ID, HASH_MD5, STU_ID, SSP_NO, SPK_NO, SPK_VER_NO, SSP_ATT_NO,
                                                CR_VAL, PARENT_SSP_NO, PARENT_SPK_NO, PARENT_SPK_VER_NO,
                                                PARENT_SSP_ATT_NO, PARENT_AVAIL_KEY, SSP_STTS_NO, SSP_STG_CD,
                                                SSP_STTS_CD, EFFCT_START_DT, PLAN_APRV_CD, PLAN_APRV_USER, PLAN_APRV_DT,
                                                PLAN_LOCATION_CD, PLAN_CHANGE_NO, NOT_ON_PLAN_FG, PLAN_COMPNT_CD,
                                                STRUCTURE_FG, STRUCT_CHG_FG, STRUCT_TEMPL_FG, STRUCT_TEMPL_NO,
                                                STRUCT_FROM_SSP_NO, COMPNT_FORM_NO, ALT_FG, ALT_COMPNT_NO,
                                                ALT_CHOSEN_FG, YR_LVL, LOCATION_CD, AVAIL_YR, SPRD_CD, AVAIL_NO,
                                                AVAIL_KEY_NO, LIAB_CAT_CD, LOAD_CAT_CD, ATTNDC_MODE_CD, STUDY_MODE_CD,
                                                FOS_CD, FLD_RESCH_CD, DISCPLN_GRP_CD, SOCIO_EC_CD, RESCH_CSWK_CD,
                                                HONS_FG, HONORARY_DEGREE_FG, NOT_ASSD_FG, SELF_ENROL_FG,
                                                SELF_WITHDRAW_FG, SELF_SELECT_FG, SELF_NONE_FG, THESIS_TITLE_DT,
                                                THESIS_TITLE, THESIS_ABSTRACT, APPN_SPK_NO, APPN_VER_NO, APPN_NO,
                                                COMP_SSP_NO, COMP_SPK_NO, COMP_SPK_VER_NO, COMP_SSP_ATT_NO, CONV_SSP_NO,
                                                CONV_SPK_NO, CONV_SPK_VER_NO, CONV_SSP_ATT_NO, CONV_DT, CONV_NOTES,
                                                ASTG_RSLT_EFFCT_DT, GRADE_CD, GRADE_TYPE_CD, PASS_FAIL_CD, MARK,
                                                EXAM_NOTES, VERIFIED_FG, RATIFIED_FG, RATIFIED_DT, CWA, AC_STTS_CD,
                                                AC_REC_REQD_FG, REQST_EXISTS, REQST_NO, COMPLN_CRIT_EXISTS,
                                                COMPLN_CRIT_NO, LOA_EXISTS, LOAD_EXISTS, GV1_YR_FST_RPT,
                                                GV4_ENROL_TYPE_CD, GV4_MOD_ENROL_CD, VERS, CRUSER, CRDATEI, CRTIMEI,
                                                CRTERM, CRWINDOW, LAST_MOD_DATEI, LAST_MOD_TIMEI, CERTIFIED_FG,
                                                CERTIFIED_DT, RSLT_EFFCT_DT, RSLT_TYPE_CD, ALT_COMPNTA_NO, FOE_CD,
                                                SCREQ_FG, REQST_WVR_FG, WVR_NO, STU_CONSOL_FG, CONSOL_TO_STU_ID,
                                                REPAR_SSP_NO, REPAR_SPK_NO, REPAR_SPK_VER_NO, REPAR_SSP_ATT_NO,
                                                LAST_EP_END_DT, HAS_BEEN_SUBS_FG, LATEST_EWS_DT, FEC_LIAB_CAT_CD,
                                                CUR_LOCATION_CD, CUR_EP_YEAR, CUR_EP_NO, STUDY_TYPE_CD, STUDY_BASIS_CD,
                                                TB_RSLT_EFFCT_DT, APPLY_RSLT_DT, STRUCT_SPK_NO, STRUCT_SPK_VER_NO,
                                                STRUCT_GRP_NO, STRUCT_GRP_SSP_NO, STRUCT_LINE_CR_VAL, STRUCT_GRP_CR_VAL,
                                                FROM_TEMPL_NO, STU_STTS_CD, STU_STTS_ASSIGN_FG, CENSUS_DT,
                                                GOVT_REPORT_DT, STU_STTS_MOD_DATEI, STU_STTS_MOD_USER, STU_STTS_MOD_WIN,
                                                GOVT_REPORT_IND, DFRL_IND, DFRL_DT, VAR_IND, GOVT_REPORT_HIST_IND,
                                                LAST_RPTBL_RVSN_NO, RMTD_IND, RMTD_DT, SELF_LOCATION_FG,
                                                SELF_ATTNDC_MODE_FG, SELF_MGMT_1_FG, SELF_MGMT_2_FG,
                                                GV1_COMPONENT_SSP_NO, GV1_PARENT_SSP_NO, MAX_ATT, GV1_PARENT_CRS_CD,
                                                PARTNER_EXT_ORG_ID, PARTNER_COMPLN_PCNT, VALUE_REMAINING, STRUCTURE_ID,
                                                STRUCTURE_CMPNT_ID, STUDY_REASON_CD, GRADE_ROLLUP_FG, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL SAT_UNIT_ENROLMENT_SK,
       MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0)
           )                                     HUB_UNIT_ENROLMENT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP.CR_VAL,  0 ) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0 ) || ',' ||
           IFNULL(UN_SSP.PARENT_SPK_NO, 0 ) || ',' ||
           IFNULL(UN_SSP.PARENT_SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_AVAIL_KEY, 0) || ',' ||
           IFNULL(UN_SSP.SSP_STTS_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_STG_CD, '') || ',' ||
           IFNULL(UN_SSP.SSP_STTS_CD, '') || ',' ||
           IFNULL(UN_SSP.EFFCT_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.PLAN_APRV_CD, '') || ',' ||
           IFNULL(UN_SSP.PLAN_APRV_USER, '') || ',' ||
           IFNULL(UN_SSP.PLAN_APRV_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.PLAN_LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.PLAN_CHANGE_NO, 0) || ',' ||
           IFNULL(UN_SSP.NOT_ON_PLAN_FG, '') || ',' ||
           IFNULL(UN_SSP.PLAN_COMPNT_CD, '') || ',' ||
           IFNULL(UN_SSP.STRUCTURE_FG, '') || ',' ||
           IFNULL(UN_SSP.STRUCT_CHG_FG, '') || ',' ||
           IFNULL(UN_SSP.STRUCT_TEMPL_FG, '') || ',' ||
           IFNULL(UN_SSP.STRUCT_TEMPL_NO, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_FROM_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMPNT_FORM_NO, 0) || ',' ||
           IFNULL(UN_SSP.ALT_FG, '') || ',' ||
           IFNULL(UN_SSP.ALT_COMPNT_NO, 0) || ',' ||
           IFNULL(UN_SSP.ALT_CHOSEN_FG, '') || ',' ||
           IFNULL(UN_SSP.YR_LVL, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.LIAB_CAT_CD, '') || ',' ||
           IFNULL(UN_SSP.LOAD_CAT_CD, '') || ',' ||
           IFNULL(UN_SSP.ATTNDC_MODE_CD, '') || ',' ||
           IFNULL(UN_SSP.STUDY_MODE_CD, '') || ',' ||
           IFNULL(UN_SSP.FOS_CD, '') || ',' ||
           IFNULL(UN_SSP.FLD_RESCH_CD, '') || ',' ||
           IFNULL(UN_SSP.DISCPLN_GRP_CD, '') || ',' ||
           IFNULL(UN_SSP.SOCIO_EC_CD, '') || ',' ||
           IFNULL(UN_SSP.RESCH_CSWK_CD, '') || ',' ||
           IFNULL(UN_SSP.HONS_FG, '') || ',' ||
           IFNULL(UN_SSP.HONORARY_DEGREE_FG, '') || ',' ||
           IFNULL(UN_SSP.NOT_ASSD_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_ENROL_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_WITHDRAW_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_SELECT_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_NONE_FG, '') || ',' ||
           IFNULL(UN_SSP.THESIS_TITLE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.THESIS_TITLE, '') || ',' ||
           IFNULL(UN_SSP.THESIS_ABSTRACT, '') || ',' ||
           IFNULL(UN_SSP.APPN_SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.APPN_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.APPN_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMP_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMP_SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMP_SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMP_SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP.CONV_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.CONV_SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.CONV_SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.CONV_SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP.CONV_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.CONV_NOTES, '') || ',' ||
           IFNULL(UN_SSP.ASTG_RSLT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.GRADE_CD, '') || ',' ||
           IFNULL(UN_SSP.GRADE_TYPE_CD, '') || ',' ||
           IFNULL(UN_SSP.PASS_FAIL_CD, '') || ',' ||
           IFNULL(UN_SSP.MARK, 0) || ',' ||
           IFNULL(UN_SSP.EXAM_NOTES, '') || ',' ||
           IFNULL(UN_SSP.VERIFIED_FG, '') || ',' ||
           IFNULL(UN_SSP.RATIFIED_FG, '') || ',' ||
           IFNULL(UN_SSP.RATIFIED_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.CWA, 0) || ',' ||
           IFNULL(UN_SSP.AC_STTS_CD, '') || ',' ||
           IFNULL(UN_SSP.AC_REC_REQD_FG, '') || ',' ||
           IFNULL(UN_SSP.REQST_EXISTS, '') || ',' ||
           IFNULL(UN_SSP.REQST_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMPLN_CRIT_EXISTS, '') || ',' ||
           IFNULL(UN_SSP.COMPLN_CRIT_NO, 0) || ',' ||
           IFNULL(UN_SSP.LOA_EXISTS, '') || ',' ||
           IFNULL(UN_SSP.LOAD_EXISTS, '') || ',' ||
           IFNULL(UN_SSP.GV1_YR_FST_RPT, 0) || ',' ||
           IFNULL(UN_SSP.GV4_ENROL_TYPE_CD, '') || ',' ||
           IFNULL(UN_SSP.GV4_MOD_ENROL_CD, '') || ',' ||
           IFNULL(UN_SSP.VERS, 0) || ',' ||
           IFNULL(UN_SSP.CRUSER, '') || ',' ||
           IFNULL(UN_SSP.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.CRTIMEI, 0) || ',' ||
           IFNULL(UN_SSP.CRTERM, '') || ',' ||
           IFNULL(UN_SSP.CRWINDOW, '') || ',' ||
           IFNULL(UN_SSP.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.LAST_MOD_TIMEI, 0) || ',' ||
           IFNULL(UN_SSP.CERTIFIED_FG, '') || ',' ||
           IFNULL(UN_SSP.CERTIFIED_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.RSLT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.RSLT_TYPE_CD, '') || ',' ||
           IFNULL(UN_SSP.ALT_COMPNTA_NO, 0) || ',' ||
           IFNULL(UN_SSP.FOE_CD, '') || ',' ||
           IFNULL(UN_SSP.SCREQ_FG, '') || ',' ||
           IFNULL(UN_SSP.REQST_WVR_FG, '') || ',' ||
           IFNULL(UN_SSP.WVR_NO, 0) || ',' ||
           IFNULL(UN_SSP.STU_CONSOL_FG, '') || ',' ||
           IFNULL(UN_SSP.CONSOL_TO_STU_ID, '') || ',' ||
           IFNULL(UN_SSP.REPAR_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.REPAR_SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.REPAR_SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.REPAR_SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP.LAST_EP_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.HAS_BEEN_SUBS_FG, '') || ',' ||
           IFNULL(UN_SSP.LATEST_EWS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.FEC_LIAB_CAT_CD, '') || ',' ||
           IFNULL(UN_SSP.CUR_LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.CUR_EP_YEAR, 0) || ',' ||
           IFNULL(UN_SSP.CUR_EP_NO, 0) || ',' ||
           IFNULL(UN_SSP.STUDY_TYPE_CD, '') || ',' ||
           IFNULL(UN_SSP.STUDY_BASIS_CD, '') || ',' ||
           IFNULL(UN_SSP.TB_RSLT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.APPLY_RSLT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.STRUCT_SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_GRP_NO, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_GRP_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_LINE_CR_VAL, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_GRP_CR_VAL, 0) || ',' ||
           IFNULL(UN_SSP.FROM_TEMPL_NO, 0) || ',' ||
           IFNULL(UN_SSP.STU_STTS_CD, '') || ',' ||
           IFNULL(UN_SSP.STU_STTS_ASSIGN_FG, '') || ',' ||
           IFNULL(UN_SSP.CENSUS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.GOVT_REPORT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.STU_STTS_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.STU_STTS_MOD_USER, '') || ',' ||
           IFNULL(UN_SSP.STU_STTS_MOD_WIN, '') || ',' ||
           IFNULL(UN_SSP.GOVT_REPORT_IND, '') || ',' ||
           IFNULL(UN_SSP.DFRL_IND, '') || ',' ||
           IFNULL(UN_SSP.DFRL_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.VAR_IND, '') || ',' ||
           IFNULL(UN_SSP.GOVT_REPORT_HIST_IND, '') || ',' ||
           IFNULL(UN_SSP.LAST_RPTBL_RVSN_NO, 0) || ',' ||
           IFNULL(UN_SSP.RMTD_IND, '') || ',' ||
           IFNULL(UN_SSP.RMTD_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.SELF_LOCATION_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_ATTNDC_MODE_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_MGMT_1_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_MGMT_2_FG, '') || ',' ||
           IFNULL(UN_SSP.GV1_COMPONENT_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.GV1_PARENT_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.MAX_ATT, 0) || ',' ||
           IFNULL(UN_SSP.GV1_PARENT_CRS_CD, '') || ',' ||
           IFNULL(UN_SSP.PARTNER_EXT_ORG_ID, 0) || ',' ||
           IFNULL(UN_SSP.PARTNER_COMPLN_PCNT, 0) || ',' ||
           IFNULL(UN_SSP.VALUE_REMAINING, 0) || ',' ||
           IFNULL(UN_SSP.STRUCTURE_ID, 0) || ',' ||
           IFNULL(UN_SSP.STRUCTURE_CMPNT_ID, 0) || ',' ||
           IFNULL(UN_SSP.STUDY_REASON_CD, '') || ',' ||
           IFNULL(UN_SSP.GRADE_ROLLUP_FG, '') || ',' ||
           IFNULL('N', '')
           )                                     HASH_MD5,
       UN_SSP.STU_ID,
       UN_SSP.SSP_NO,
       UN_SSP.SPK_NO,
       UN_SSP.SPK_VER_NO,
       UN_SSP.SSP_ATT_NO,
       UN_SSP.CR_VAL,
       UN_SSP.PARENT_SSP_NO,
       UN_SSP.PARENT_SPK_NO,
       UN_SSP.PARENT_SPK_VER_NO,
       UN_SSP.PARENT_SSP_ATT_NO,
       UN_SSP.PARENT_AVAIL_KEY,
       UN_SSP.SSP_STTS_NO,
       UN_SSP.SSP_STG_CD,
       UN_SSP.SSP_STTS_CD,
       UN_SSP.EFFCT_START_DT,
       UN_SSP.PLAN_APRV_CD,
       UN_SSP.PLAN_APRV_USER,
       UN_SSP.PLAN_APRV_DT,
       UN_SSP.PLAN_LOCATION_CD,
       UN_SSP.PLAN_CHANGE_NO,
       UN_SSP.NOT_ON_PLAN_FG,
       UN_SSP.PLAN_COMPNT_CD,
       UN_SSP.STRUCTURE_FG,
       UN_SSP.STRUCT_CHG_FG,
       UN_SSP.STRUCT_TEMPL_FG,
       UN_SSP.STRUCT_TEMPL_NO,
       UN_SSP.STRUCT_FROM_SSP_NO,
       UN_SSP.COMPNT_FORM_NO,
       UN_SSP.ALT_FG,
       UN_SSP.ALT_COMPNT_NO,
       UN_SSP.ALT_CHOSEN_FG,
       UN_SSP.YR_LVL,
       UN_SSP.LOCATION_CD,
       UN_SSP.AVAIL_YR,
       UN_SSP.SPRD_CD,
       UN_SSP.AVAIL_NO,
       UN_SSP.AVAIL_KEY_NO,
       UN_SSP.LIAB_CAT_CD,
       UN_SSP.LOAD_CAT_CD,
       UN_SSP.ATTNDC_MODE_CD,
       UN_SSP.STUDY_MODE_CD,
       UN_SSP.FOS_CD,
       UN_SSP.FLD_RESCH_CD,
       UN_SSP.DISCPLN_GRP_CD,
       UN_SSP.SOCIO_EC_CD,
       UN_SSP.RESCH_CSWK_CD,
       UN_SSP.HONS_FG,
       UN_SSP.HONORARY_DEGREE_FG,
       UN_SSP.NOT_ASSD_FG,
       UN_SSP.SELF_ENROL_FG,
       UN_SSP.SELF_WITHDRAW_FG,
       UN_SSP.SELF_SELECT_FG,
       UN_SSP.SELF_NONE_FG,
       UN_SSP.THESIS_TITLE_DT,
       UN_SSP.THESIS_TITLE,
       UN_SSP.THESIS_ABSTRACT,
       UN_SSP.APPN_SPK_NO,
       UN_SSP.APPN_VER_NO,
       UN_SSP.APPN_NO,
       UN_SSP.COMP_SSP_NO,
       UN_SSP.COMP_SPK_NO,
       UN_SSP.COMP_SPK_VER_NO,
       UN_SSP.COMP_SSP_ATT_NO,
       UN_SSP.CONV_SSP_NO,
       UN_SSP.CONV_SPK_NO,
       UN_SSP.CONV_SPK_VER_NO,
       UN_SSP.CONV_SSP_ATT_NO,
       UN_SSP.CONV_DT,
       UN_SSP.CONV_NOTES,
       UN_SSP.ASTG_RSLT_EFFCT_DT,
       UN_SSP.GRADE_CD,
       UN_SSP.GRADE_TYPE_CD,
       UN_SSP.PASS_FAIL_CD,
       UN_SSP.MARK,
       UN_SSP.EXAM_NOTES,
       UN_SSP.VERIFIED_FG,
       UN_SSP.RATIFIED_FG,
       UN_SSP.RATIFIED_DT,
       UN_SSP.CWA,
       UN_SSP.AC_STTS_CD,
       UN_SSP.AC_REC_REQD_FG,
       UN_SSP.REQST_EXISTS,
       UN_SSP.REQST_NO,
       UN_SSP.COMPLN_CRIT_EXISTS,
       UN_SSP.COMPLN_CRIT_NO,
       UN_SSP.LOA_EXISTS,
       UN_SSP.LOAD_EXISTS,
       UN_SSP.GV1_YR_FST_RPT,
       UN_SSP.GV4_ENROL_TYPE_CD,
       UN_SSP.GV4_MOD_ENROL_CD,
       UN_SSP.VERS,
       UN_SSP.CRUSER,
       UN_SSP.CRDATEI,
       UN_SSP.CRTIMEI,
       UN_SSP.CRTERM,
       UN_SSP.CRWINDOW,
       UN_SSP.LAST_MOD_DATEI,
       UN_SSP.LAST_MOD_TIMEI,
       UN_SSP.CERTIFIED_FG,
       UN_SSP.CERTIFIED_DT,
       UN_SSP.RSLT_EFFCT_DT,
       UN_SSP.RSLT_TYPE_CD,
       UN_SSP.ALT_COMPNTA_NO,
       UN_SSP.FOE_CD,
       UN_SSP.SCREQ_FG,
       UN_SSP.REQST_WVR_FG,
       UN_SSP.WVR_NO,
       UN_SSP.STU_CONSOL_FG,
       UN_SSP.CONSOL_TO_STU_ID,
       UN_SSP.REPAR_SSP_NO,
       UN_SSP.REPAR_SPK_NO,
       UN_SSP.REPAR_SPK_VER_NO,
       UN_SSP.REPAR_SSP_ATT_NO,
       UN_SSP.LAST_EP_END_DT,
       UN_SSP.HAS_BEEN_SUBS_FG,
       UN_SSP.LATEST_EWS_DT,
       UN_SSP.FEC_LIAB_CAT_CD,
       UN_SSP.CUR_LOCATION_CD,
       UN_SSP.CUR_EP_YEAR,
       UN_SSP.CUR_EP_NO,
       UN_SSP.STUDY_TYPE_CD,
       UN_SSP.STUDY_BASIS_CD,
       UN_SSP.TB_RSLT_EFFCT_DT,
       UN_SSP.APPLY_RSLT_DT,
       UN_SSP.STRUCT_SPK_NO,
       UN_SSP.STRUCT_SPK_VER_NO,
       UN_SSP.STRUCT_GRP_NO,
       UN_SSP.STRUCT_GRP_SSP_NO,
       UN_SSP.STRUCT_LINE_CR_VAL,
       UN_SSP.STRUCT_GRP_CR_VAL,
       UN_SSP.FROM_TEMPL_NO,
       UN_SSP.STU_STTS_CD,
       UN_SSP.STU_STTS_ASSIGN_FG,
       UN_SSP.CENSUS_DT,
       UN_SSP.GOVT_REPORT_DT,
       UN_SSP.STU_STTS_MOD_DATEI,
       UN_SSP.STU_STTS_MOD_USER,
       UN_SSP.STU_STTS_MOD_WIN,
       UN_SSP.GOVT_REPORT_IND,
       UN_SSP.DFRL_IND,
       UN_SSP.DFRL_DT,
       UN_SSP.VAR_IND,
       UN_SSP.GOVT_REPORT_HIST_IND,
       UN_SSP.LAST_RPTBL_RVSN_NO,
       UN_SSP.RMTD_IND,
       UN_SSP.RMTD_DT,
       UN_SSP.SELF_LOCATION_FG,
       UN_SSP.SELF_ATTNDC_MODE_FG,
       UN_SSP.SELF_MGMT_1_FG,
       UN_SSP.SELF_MGMT_2_FG,
       UN_SSP.GV1_COMPONENT_SSP_NO,
       UN_SSP.GV1_PARENT_SSP_NO,
       UN_SSP.MAX_ATT,
       UN_SSP.GV1_PARENT_CRS_CD,
       UN_SSP.PARTNER_EXT_ORG_ID,
       UN_SSP.PARTNER_COMPLN_PCNT,
       UN_SSP.VALUE_REMAINING,
       UN_SSP.STRUCTURE_ID,
       UN_SSP.STRUCTURE_CMPNT_ID,
       UN_SSP.STUDY_REASON_CD,
       UN_SSP.GRADE_ROLLUP_FG,
       'N'                                       IS_DELETED
FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_UNIT_ENROLMENT_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_UNIT_ENROLMENT_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_UNIT_ENROLMENT) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_UNIT_ENROLMENT_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                             IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                                             IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                                             IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                                             IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                                             IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.PARENT_SSP_NO, 0)
            )
          AND S.HASH_MD5 = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP.CR_VAL,  0 ) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0 ) || ',' ||
           IFNULL(UN_SSP.PARENT_SPK_NO, 0 ) || ',' ||
           IFNULL(UN_SSP.PARENT_SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_AVAIL_KEY, 0) || ',' ||
           IFNULL(UN_SSP.SSP_STTS_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_STG_CD, '') || ',' ||
           IFNULL(UN_SSP.SSP_STTS_CD, '') || ',' ||
           IFNULL(UN_SSP.EFFCT_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.PLAN_APRV_CD, '') || ',' ||
           IFNULL(UN_SSP.PLAN_APRV_USER, '') || ',' ||
           IFNULL(UN_SSP.PLAN_APRV_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.PLAN_LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.PLAN_CHANGE_NO, 0) || ',' ||
           IFNULL(UN_SSP.NOT_ON_PLAN_FG, '') || ',' ||
           IFNULL(UN_SSP.PLAN_COMPNT_CD, '') || ',' ||
           IFNULL(UN_SSP.STRUCTURE_FG, '') || ',' ||
           IFNULL(UN_SSP.STRUCT_CHG_FG, '') || ',' ||
           IFNULL(UN_SSP.STRUCT_TEMPL_FG, '') || ',' ||
           IFNULL(UN_SSP.STRUCT_TEMPL_NO, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_FROM_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMPNT_FORM_NO, 0) || ',' ||
           IFNULL(UN_SSP.ALT_FG, '') || ',' ||
           IFNULL(UN_SSP.ALT_COMPNT_NO, 0) || ',' ||
           IFNULL(UN_SSP.ALT_CHOSEN_FG, '') || ',' ||
           IFNULL(UN_SSP.YR_LVL, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.LIAB_CAT_CD, '') || ',' ||
           IFNULL(UN_SSP.LOAD_CAT_CD, '') || ',' ||
           IFNULL(UN_SSP.ATTNDC_MODE_CD, '') || ',' ||
           IFNULL(UN_SSP.STUDY_MODE_CD, '') || ',' ||
           IFNULL(UN_SSP.FOS_CD, '') || ',' ||
           IFNULL(UN_SSP.FLD_RESCH_CD, '') || ',' ||
           IFNULL(UN_SSP.DISCPLN_GRP_CD, '') || ',' ||
           IFNULL(UN_SSP.SOCIO_EC_CD, '') || ',' ||
           IFNULL(UN_SSP.RESCH_CSWK_CD, '') || ',' ||
           IFNULL(UN_SSP.HONS_FG, '') || ',' ||
           IFNULL(UN_SSP.HONORARY_DEGREE_FG, '') || ',' ||
           IFNULL(UN_SSP.NOT_ASSD_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_ENROL_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_WITHDRAW_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_SELECT_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_NONE_FG, '') || ',' ||
           IFNULL(UN_SSP.THESIS_TITLE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.THESIS_TITLE, '') || ',' ||
           IFNULL(UN_SSP.THESIS_ABSTRACT, '') || ',' ||
           IFNULL(UN_SSP.APPN_SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.APPN_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.APPN_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMP_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMP_SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMP_SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMP_SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP.CONV_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.CONV_SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.CONV_SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.CONV_SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP.CONV_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.CONV_NOTES, '') || ',' ||
           IFNULL(UN_SSP.ASTG_RSLT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.GRADE_CD, '') || ',' ||
           IFNULL(UN_SSP.GRADE_TYPE_CD, '') || ',' ||
           IFNULL(UN_SSP.PASS_FAIL_CD, '') || ',' ||
           IFNULL(UN_SSP.MARK, 0) || ',' ||
           IFNULL(UN_SSP.EXAM_NOTES, '') || ',' ||
           IFNULL(UN_SSP.VERIFIED_FG, '') || ',' ||
           IFNULL(UN_SSP.RATIFIED_FG, '') || ',' ||
           IFNULL(UN_SSP.RATIFIED_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.CWA, 0) || ',' ||
           IFNULL(UN_SSP.AC_STTS_CD, '') || ',' ||
           IFNULL(UN_SSP.AC_REC_REQD_FG, '') || ',' ||
           IFNULL(UN_SSP.REQST_EXISTS, '') || ',' ||
           IFNULL(UN_SSP.REQST_NO, 0) || ',' ||
           IFNULL(UN_SSP.COMPLN_CRIT_EXISTS, '') || ',' ||
           IFNULL(UN_SSP.COMPLN_CRIT_NO, 0) || ',' ||
           IFNULL(UN_SSP.LOA_EXISTS, '') || ',' ||
           IFNULL(UN_SSP.LOAD_EXISTS, '') || ',' ||
           IFNULL(UN_SSP.GV1_YR_FST_RPT, 0) || ',' ||
           IFNULL(UN_SSP.GV4_ENROL_TYPE_CD, '') || ',' ||
           IFNULL(UN_SSP.GV4_MOD_ENROL_CD, '') || ',' ||
           IFNULL(UN_SSP.VERS, 0) || ',' ||
           IFNULL(UN_SSP.CRUSER, '') || ',' ||
           IFNULL(UN_SSP.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.CRTIMEI, 0) || ',' ||
           IFNULL(UN_SSP.CRTERM, '') || ',' ||
           IFNULL(UN_SSP.CRWINDOW, '') || ',' ||
           IFNULL(UN_SSP.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.LAST_MOD_TIMEI, 0) || ',' ||
           IFNULL(UN_SSP.CERTIFIED_FG, '') || ',' ||
           IFNULL(UN_SSP.CERTIFIED_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.RSLT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.RSLT_TYPE_CD, '') || ',' ||
           IFNULL(UN_SSP.ALT_COMPNTA_NO, 0) || ',' ||
           IFNULL(UN_SSP.FOE_CD, '') || ',' ||
           IFNULL(UN_SSP.SCREQ_FG, '') || ',' ||
           IFNULL(UN_SSP.REQST_WVR_FG, '') || ',' ||
           IFNULL(UN_SSP.WVR_NO, 0) || ',' ||
           IFNULL(UN_SSP.STU_CONSOL_FG, '') || ',' ||
           IFNULL(UN_SSP.CONSOL_TO_STU_ID, '') || ',' ||
           IFNULL(UN_SSP.REPAR_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.REPAR_SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.REPAR_SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.REPAR_SSP_ATT_NO, 0) || ',' ||
           IFNULL(UN_SSP.LAST_EP_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.HAS_BEEN_SUBS_FG, '') || ',' ||
           IFNULL(UN_SSP.LATEST_EWS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.FEC_LIAB_CAT_CD, '') || ',' ||
           IFNULL(UN_SSP.CUR_LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.CUR_EP_YEAR, 0) || ',' ||
           IFNULL(UN_SSP.CUR_EP_NO, 0) || ',' ||
           IFNULL(UN_SSP.STUDY_TYPE_CD, '') || ',' ||
           IFNULL(UN_SSP.STUDY_BASIS_CD, '') || ',' ||
           IFNULL(UN_SSP.TB_RSLT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.APPLY_RSLT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.STRUCT_SPK_NO, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_GRP_NO, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_GRP_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_LINE_CR_VAL, 0) || ',' ||
           IFNULL(UN_SSP.STRUCT_GRP_CR_VAL, 0) || ',' ||
           IFNULL(UN_SSP.FROM_TEMPL_NO, 0) || ',' ||
           IFNULL(UN_SSP.STU_STTS_CD, '') || ',' ||
           IFNULL(UN_SSP.STU_STTS_ASSIGN_FG, '') || ',' ||
           IFNULL(UN_SSP.CENSUS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.GOVT_REPORT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.STU_STTS_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.STU_STTS_MOD_USER, '') || ',' ||
           IFNULL(UN_SSP.STU_STTS_MOD_WIN, '') || ',' ||
           IFNULL(UN_SSP.GOVT_REPORT_IND, '') || ',' ||
           IFNULL(UN_SSP.DFRL_IND, '') || ',' ||
           IFNULL(UN_SSP.DFRL_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.VAR_IND, '') || ',' ||
           IFNULL(UN_SSP.GOVT_REPORT_HIST_IND, '') || ',' ||
           IFNULL(UN_SSP.LAST_RPTBL_RVSN_NO, 0) || ',' ||
           IFNULL(UN_SSP.RMTD_IND, '') || ',' ||
           IFNULL(UN_SSP.RMTD_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(UN_SSP.SELF_LOCATION_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_ATTNDC_MODE_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_MGMT_1_FG, '') || ',' ||
           IFNULL(UN_SSP.SELF_MGMT_2_FG, '') || ',' ||
           IFNULL(UN_SSP.GV1_COMPONENT_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.GV1_PARENT_SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.MAX_ATT, 0) || ',' ||
           IFNULL(UN_SSP.GV1_PARENT_CRS_CD, '') || ',' ||
           IFNULL(UN_SSP.PARTNER_EXT_ORG_ID, 0) || ',' ||
           IFNULL(UN_SSP.PARTNER_COMPLN_PCNT, 0) || ',' ||
           IFNULL(UN_SSP.VALUE_REMAINING, 0) || ',' ||
           IFNULL(UN_SSP.STRUCTURE_ID, 0) || ',' ||
           IFNULL(UN_SSP.STRUCTURE_CMPNT_ID, 0) || ',' ||
           IFNULL(UN_SSP.STUDY_REASON_CD, '') || ',' ||
           IFNULL(UN_SSP.GRADE_ROLLUP_FG, '') || ',' ||
           IFNULL('N', '')
           )
    )
;