-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_STUDENT_DET (SAT_STUDENT_DET_SK, HUB_STUDENT_KEY, SOURCE, LOAD_DTS, ETL_JOB_ID,
                                             HASH_MD5, STU_ID, STU_FAMILY_NM, STU_GVN_NM, STU_OTH_NM, STU_TITLE_CD,
                                             STU_ALIAS, STU_PREF_NM, STU_FORMAL_NM_1, STU_FORMAL_NM_2, STU_BIRTH_DT,
                                             STU_GENDER_CD, STU_INITS, STU_DECD_FG, STU_CITIZEN_CD, STU_CTZN_CONF_DT,
                                             STU_PRM_RSD_CD, STU_PRM_RSD_DT, STU_PRM_RSD_MET_CD, STU_RSD_AUST_CD,
                                             STU_RSD_OUT_AUS_CD, STU_BIRTH_CNTRY_CD, STU_ENTRY_YR, STU_HOME_LANG_CD,
                                             STU_ABOR_TSI_CD, CURR_EMP_STTS_CD, SCH_LVL_COM_CD, AT_SCHOOL_FG,
                                             GV4_STUDY_REAS_CD, CRDATEI, CRTIMEI, LAST_MOD_DATEI, LAST_MOD_TIMEI,
                                             STU_YOE_UNKNOWN_FG, STU_CONSOL_FG, CONSOL_TO_STU_ID, CITIZEN_EFFCT_DT,
                                             NAME_EFFCT_DT, NAME_CHG_RSN_CD, STU_CTZN_CNTRY_CD, YR12_EXT_ORG_ID,
                                             YR12_STU_ID, YR12_YEAR, YR12_STATE, STU_HLA_CODE, STU_HLA_YEAR,
                                             STU_HLA_MOD_DT, YR12_COMP_FG, STU_DISAB_FG, STU_DISAB_INFO_FG,
                                             STU_PREF_CORR_METH_CD, STU_CTZN_SELF_NOM_FG, PREF_CONTACT_METH_CD,
                                             YR12_RSLT_TYPE_CD, STU_PROF_SPOKEN_CD, SCH_LVL_COM_YEAR,
                                             CURR_SCHOOL_EXT_ORG_ID, STU_DUAL_CTZN_CNTRY_CD, VISA_TYPE_CD,
                                             VISA_SUB_TYPE_CD, STATUS_CD, NATIONALITY_CD, LENGTH_OF_RESIDENCE_CD,
                                             PURPOSE_OF_RESIDENCE, TEMPORARY_ABSENCE_CD, RESIDENTIAL_CATEGORY_CD,
                                             COUNTRY_ORDINARY_RESIDENCE, IS_DELETED)
SELECT CORE.SEQ.NEXTVAL                          SAT_STUDENT_DET_SK,
       MD5(STU_DET.STU_ID)                       HUB_STUDENT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(STU_DET.STU_ID, '') || ',' ||
           IFNULL(STU_DET.STU_FAMILY_NM, '') || ',' ||
           IFNULL(STU_DET.STU_GVN_NM, '') || ',' ||
           IFNULL(STU_DET.STU_OTH_NM, '') || ',' ||
           IFNULL(STU_DET.STU_TITLE_CD, '') || ',' ||
           IFNULL(STU_DET.STU_ALIAS, '') || ',' ||
           IFNULL(STU_DET.STU_PREF_NM, '') || ',' ||
           IFNULL(STU_DET.STU_FORMAL_NM_1, '') || ',' ||
           IFNULL(STU_DET.STU_FORMAL_NM_2, '') || ',' ||
           IFNULL(STU_DET.STU_BIRTH_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_DET.STU_GENDER_CD, '') || ',' ||
           IFNULL(STU_DET.STU_INITS, '') || ',' ||
           IFNULL(STU_DET.STU_DECD_FG, '') || ',' ||
           IFNULL(STU_DET.STU_CITIZEN_CD, '') || ',' ||
           IFNULL(STU_DET.STU_CTZN_CONF_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_DET.STU_PRM_RSD_CD, '') || ',' ||
           IFNULL(STU_DET.STU_PRM_RSD_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_DET.STU_PRM_RSD_MET_CD, '') || ',' ||
           IFNULL(STU_DET.STU_RSD_AUST_CD, '') || ',' ||
           IFNULL(STU_DET.STU_RSD_OUT_AUS_CD, '') || ',' ||
           IFNULL(STU_DET.STU_BIRTH_CNTRY_CD, '') || ',' ||
           IFNULL(STU_DET.STU_ENTRY_YR, 0) || ',' ||
           IFNULL(STU_DET.STU_HOME_LANG_CD, '') || ',' ||
           IFNULL(STU_DET.STU_ABOR_TSI_CD, '') || ',' ||
           IFNULL(STU_DET.CURR_EMP_STTS_CD, '') || ',' ||
           IFNULL(STU_DET.SCH_LVL_COM_CD, '') || ',' ||
           IFNULL(STU_DET.AT_SCHOOL_FG, '') || ',' ||
           IFNULL(STU_DET.GV4_STUDY_REAS_CD, '') || ',' ||
           IFNULL(STU_DET.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_DET.CRTIMEI, 0) || ',' ||
           IFNULL(STU_DET.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_DET.LAST_MOD_TIMEI, 0) || ',' ||
           IFNULL(STU_DET.STU_YOE_UNKNOWN_FG, '') || ',' ||
           IFNULL(STU_DET.STU_CONSOL_FG, '') || ',' ||
           IFNULL(STU_DET.CONSOL_TO_STU_ID, '') || ',' ||
           IFNULL(STU_DET.CITIZEN_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_DET.NAME_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_DET.NAME_CHG_RSN_CD, '') || ',' ||
           IFNULL(STU_DET.STU_CTZN_CNTRY_CD, '') || ',' ||
           IFNULL(STU_DET.YR12_EXT_ORG_ID, 0) || ',' ||
           IFNULL(STU_DET.YR12_STU_ID, '') || ',' ||
           IFNULL(STU_DET.YR12_YEAR, '') || ',' ||
           IFNULL(STU_DET.YR12_STATE, '') || ',' ||
           IFNULL(STU_DET.STU_HLA_CODE, '') || ',' ||
           IFNULL(STU_DET.STU_HLA_YEAR, '') || ',' ||
           IFNULL(STU_DET.STU_HLA_MOD_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_DET.YR12_COMP_FG, '') || ',' ||
           IFNULL(STU_DET.STU_DISAB_FG, '') || ',' ||
           IFNULL(STU_DET.STU_DISAB_INFO_FG, '') || ',' ||
           IFNULL(STU_DET.STU_PREF_CORR_METH_CD, '') || ',' ||
           IFNULL(STU_DET.STU_CTZN_SELF_NOM_FG, '') || ',' ||
           IFNULL(STU_DET.PREF_CONTACT_METH_CD, '') || ',' ||
           IFNULL(STU_DET.YR12_RSLT_TYPE_CD, '') || ',' ||
           IFNULL(STU_DET.STU_PROF_SPOKEN_CD, '') || ',' ||
           IFNULL(STU_DET.SCH_LVL_COM_YEAR, 0) || ',' ||
           IFNULL(STU_DET.CURR_SCHOOL_EXT_ORG_ID, 0) || ',' ||
           IFNULL(STU_DET.STU_DUAL_CTZN_CNTRY_CD, '') || ',' ||
           IFNULL(STU_DET.VISA_TYPE_CD, '') || ',' ||
           IFNULL(STU_DET.VISA_SUB_TYPE_CD, '') || ',' ||
           IFNULL(STU_DET.STATUS_CD, '') || ',' ||
           IFNULL(STU_DET.NATIONALITY_CD, '') || ',' ||
           IFNULL(STU_DET.LENGTH_OF_RESIDENCE_CD, '') || ',' ||
           IFNULL(STU_DET.PURPOSE_OF_RESIDENCE, '') || ',' ||
           IFNULL(STU_DET.TEMPORARY_ABSENCE_CD, '') || ',' ||
           IFNULL(STU_DET.RESIDENTIAL_CATEGORY_CD, '') || ',' ||
           IFNULL(STU_DET.COUNTRY_ORDINARY_RESIDENCE, '') || ',' ||
           IFNULL('N', '')
           )                                     HASH_MD5,
       STU_DET.STU_ID,
       STU_DET.STU_FAMILY_NM,
       STU_DET.STU_GVN_NM,
       STU_DET.STU_OTH_NM,
       STU_DET.STU_TITLE_CD,
       STU_DET.STU_ALIAS,
       STU_DET.STU_PREF_NM,
       STU_DET.STU_FORMAL_NM_1,
       STU_DET.STU_FORMAL_NM_2,
       STU_DET.STU_BIRTH_DT,
       STU_DET.STU_GENDER_CD,
       STU_DET.STU_INITS,
       STU_DET.STU_DECD_FG,
       STU_DET.STU_CITIZEN_CD,
       STU_DET.STU_CTZN_CONF_DT,
       STU_DET.STU_PRM_RSD_CD,
       STU_DET.STU_PRM_RSD_DT,
       STU_DET.STU_PRM_RSD_MET_CD,
       STU_DET.STU_RSD_AUST_CD,
       STU_DET.STU_RSD_OUT_AUS_CD,
       STU_DET.STU_BIRTH_CNTRY_CD,
       STU_DET.STU_ENTRY_YR,
       STU_DET.STU_HOME_LANG_CD,
       STU_DET.STU_ABOR_TSI_CD,
       STU_DET.CURR_EMP_STTS_CD,
       STU_DET.SCH_LVL_COM_CD,
       STU_DET.AT_SCHOOL_FG,
       STU_DET.GV4_STUDY_REAS_CD,
       STU_DET.CRDATEI,
       STU_DET.CRTIMEI,
       STU_DET.LAST_MOD_DATEI,
       STU_DET.LAST_MOD_TIMEI,
       STU_DET.STU_YOE_UNKNOWN_FG,
       STU_DET.STU_CONSOL_FG,
       STU_DET.CONSOL_TO_STU_ID,
       STU_DET.CITIZEN_EFFCT_DT,
       STU_DET.NAME_EFFCT_DT,
       STU_DET.NAME_CHG_RSN_CD,
       STU_DET.STU_CTZN_CNTRY_CD,
       STU_DET.YR12_EXT_ORG_ID,
       STU_DET.YR12_STU_ID,
       STU_DET.YR12_YEAR,
       STU_DET.YR12_STATE,
       STU_DET.STU_HLA_CODE,
       STU_DET.STU_HLA_YEAR,
       STU_DET.STU_HLA_MOD_DT,
       STU_DET.YR12_COMP_FG,
       STU_DET.STU_DISAB_FG,
       STU_DET.STU_DISAB_INFO_FG,
       STU_DET.STU_PREF_CORR_METH_CD,
       STU_DET.STU_CTZN_SELF_NOM_FG,
       STU_DET.PREF_CONTACT_METH_CD,
       STU_DET.YR12_RSLT_TYPE_CD,
       STU_DET.STU_PROF_SPOKEN_CD,
       STU_DET.SCH_LVL_COM_YEAR,
       STU_DET.CURR_SCHOOL_EXT_ORG_ID,
       STU_DET.STU_DUAL_CTZN_CNTRY_CD,
       STU_DET.VISA_TYPE_CD,
       STU_DET.VISA_SUB_TYPE_CD,
       STU_DET.STATUS_CD,
       STU_DET.NATIONALITY_CD,
       STU_DET.LENGTH_OF_RESIDENCE_CD,
       STU_DET.PURPOSE_OF_RESIDENCE,
       STU_DET.TEMPORARY_ABSENCE_CD,
       STU_DET.RESIDENTIAL_CATEGORY_CD,
       STU_DET.COUNTRY_ORDINARY_RESIDENCE,
       'N'                                       IS_DELETED
FROM ODS.AMIS.S1STU_DET STU_DET
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_STUDENT_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER(PARTITION BY HUB_STUDENT_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_STUDENT_DET) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_STUDENT_KEY = MD5(STU_DET.STU_ID)
          AND S.HASH_MD5 = MD5(IFNULL(STU_DET.STU_ID, '') || ',' ||
                               IFNULL(STU_DET.STU_FAMILY_NM, '') || ',' ||
                               IFNULL(STU_DET.STU_GVN_NM, '') || ',' ||
                               IFNULL(STU_DET.STU_OTH_NM, '') || ',' ||
                               IFNULL(STU_DET.STU_TITLE_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_ALIAS, '') || ',' ||
                               IFNULL(STU_DET.STU_PREF_NM, '') || ',' ||
                               IFNULL(STU_DET.STU_FORMAL_NM_1, '') || ',' ||
                               IFNULL(STU_DET.STU_FORMAL_NM_2, '') || ',' ||
                               IFNULL(STU_DET.STU_BIRTH_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_DET.STU_GENDER_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_INITS, '') || ',' ||
                               IFNULL(STU_DET.STU_DECD_FG, '') || ',' ||
                               IFNULL(STU_DET.STU_CITIZEN_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_CTZN_CONF_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_DET.STU_PRM_RSD_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_PRM_RSD_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_DET.STU_PRM_RSD_MET_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_RSD_AUST_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_RSD_OUT_AUS_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_BIRTH_CNTRY_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_ENTRY_YR, 0) || ',' ||
                               IFNULL(STU_DET.STU_HOME_LANG_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_ABOR_TSI_CD, '') || ',' ||
                               IFNULL(STU_DET.CURR_EMP_STTS_CD, '') || ',' ||
                               IFNULL(STU_DET.SCH_LVL_COM_CD, '') || ',' ||
                               IFNULL(STU_DET.AT_SCHOOL_FG, '') || ',' ||
                               IFNULL(STU_DET.GV4_STUDY_REAS_CD, '') || ',' ||
                               IFNULL(STU_DET.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_DET.CRTIMEI, 0) || ',' ||
                               IFNULL(STU_DET.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_DET.LAST_MOD_TIMEI, 0) || ',' ||
                               IFNULL(STU_DET.STU_YOE_UNKNOWN_FG, '') || ',' ||
                               IFNULL(STU_DET.STU_CONSOL_FG, '') || ',' ||
                               IFNULL(STU_DET.CONSOL_TO_STU_ID, '') || ',' ||
                               IFNULL(STU_DET.CITIZEN_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_DET.NAME_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_DET.NAME_CHG_RSN_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_CTZN_CNTRY_CD, '') || ',' ||
                               IFNULL(STU_DET.YR12_EXT_ORG_ID, 0) || ',' ||
                               IFNULL(STU_DET.YR12_STU_ID, '') || ',' ||
                               IFNULL(STU_DET.YR12_YEAR, '') || ',' ||
                               IFNULL(STU_DET.YR12_STATE, '') || ',' ||
                               IFNULL(STU_DET.STU_HLA_CODE, '') || ',' ||
                               IFNULL(STU_DET.STU_HLA_YEAR, '') || ',' ||
                               IFNULL(STU_DET.STU_HLA_MOD_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_DET.YR12_COMP_FG, '') || ',' ||
                               IFNULL(STU_DET.STU_DISAB_FG, '') || ',' ||
                               IFNULL(STU_DET.STU_DISAB_INFO_FG, '') || ',' ||
                               IFNULL(STU_DET.STU_PREF_CORR_METH_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_CTZN_SELF_NOM_FG, '') || ',' ||
                               IFNULL(STU_DET.PREF_CONTACT_METH_CD, '') || ',' ||
                               IFNULL(STU_DET.YR12_RSLT_TYPE_CD, '') || ',' ||
                               IFNULL(STU_DET.STU_PROF_SPOKEN_CD, '') || ',' ||
                               IFNULL(STU_DET.SCH_LVL_COM_YEAR, 0) || ',' ||
                               IFNULL(STU_DET.CURR_SCHOOL_EXT_ORG_ID, 0) || ',' ||
                               IFNULL(STU_DET.STU_DUAL_CTZN_CNTRY_CD, '') || ',' ||
                               IFNULL(STU_DET.VISA_TYPE_CD, '') || ',' ||
                               IFNULL(STU_DET.VISA_SUB_TYPE_CD, '') || ',' ||
                               IFNULL(STU_DET.STATUS_CD, '') || ',' ||
                               IFNULL(STU_DET.NATIONALITY_CD, '') || ',' ||
                               IFNULL(STU_DET.LENGTH_OF_RESIDENCE_CD, '') || ',' ||
                               IFNULL(STU_DET.PURPOSE_OF_RESIDENCE, '') || ',' ||
                               IFNULL(STU_DET.TEMPORARY_ABSENCE_CD, '') || ',' ||
                               IFNULL(STU_DET.RESIDENTIAL_CATEGORY_CD, '') || ',' ||
                               IFNULL(STU_DET.COUNTRY_ORDINARY_RESIDENCE, '') || ',' ||
                               IFNULL('N', '')
            )
    )
;

