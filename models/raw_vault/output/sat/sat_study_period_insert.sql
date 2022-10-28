INSERT INTO DATA_VAULT.CORE.SAT_STUDY_PERIOD (SAT_STUDY_PERIOD_SK,
                                              HUB_STUDY_PERIOD_KEY,
                                              SOURCE,
                                              LOAD_DTS,
                                              ETL_JOB_ID,
                                              HASH_MD5,
                                              CALDR_YR,
                                              LOCATION_CD,
                                              SPRD_CD,
                                              CAMPUS,
                                              STUDY_PERIOD,
                                              START_DT,
                                              END_DT,
                                              ADMIT_ON_OFFER_ACCEPTANCE_CUT_OFF_DATE,
                                              EARLIEST_ADMISSION_DATE,
                                              FIRST_APPN_DATE_VIA_EAPPS_DATE,
                                              LAST_APPN_DATE_VIA_EAPPS_DATE,
                                              LAST_WITHDRAW_DATE_VIA_EAPPS_DATE,
                                              EARLIEST_ENROLMENT_VIA_ESTUDENT_DATE,
                                              FIRST_EXPANSION_DATE,
                                              LAST_WITHDRAWAL_WITHOUT_FAIL_DATE,
                                              LAST_ADMISSION_DATE,
                                              LAST_APPLICATION_DATE,
                                              LAST_ENROLMENT_DATE,
                                              LAST_DATE_FOR_RESULT_ENTRY_DATE,
                                              LAST_WITHDRAWAL_DATE,
                                              RESULT_PUBLICATION_DATE,
                                              STUDY_PERIOD_START_DATE,
                                              TEACHING_CENSUS_DATE,
                                              IS_DELETED)

SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_STUDY_PERIOD_SK,
       MD5(IFNULL(SP_DETAILS.CALDR_YR, 0) || ',' ||
           IFNULL(SP_DETAILS.LOCATION_CD, '') || ',' ||
           IFNULL(SP_DETAILS.SPRD_CD, ''))       HUB_STUDY_PERIOD_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(SP_DETAILS.CALDR_YR, 0) || ',' ||
           IFNULL(SP_DETAILS.LOCATION_CD, '') || ',' ||
           IFNULL(SP_DETAILS.SPRD_CD, '') || ',' ||
           IFNULL(SP_DETAILS.CAMPUS, '') || ',' ||
           IFNULL(SP_DETAILS.STUDY_PERIOD, '') || ',' ||
           IFNULL(SP_DETAILS.START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.ADMIT_ON_OFFER_ACCEPTANCE_CUT_OFF_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.EARLIEST_ADMISSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.FIRST_APPN_DATE_VIA_EAPPS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.LAST_APPN_DATE_VIA_EAPPS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.LAST_WITHDRAW_DATE_VIA_EAPPS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.EARLIEST_ENROLMENT_VIA_ESTUDENT_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.FIRST_EXPANSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.LAST_WITHDRAWAL_WITHOUT_FAIL_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.LAST_ADMISSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.LAST_APPLICATION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.LAST_ENROLMENT_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.LAST_DATE_FOR_RESULT_ENTRY_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.LAST_WITHDRAWAL_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.RESULT_PUBLICATION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.STUDY_PERIOD_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SP_DETAILS.TEACHING_CENSUS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL('N', '')
           )                                     HASH_MD5,
       SP_DETAILS.CALDR_YR,
       SP_DETAILS.LOCATION_CD,
       SP_DETAILS.SPRD_CD,
       SP_DETAILS.CAMPUS,
       SP_DETAILS.STUDY_PERIOD,
       SP_DETAILS.START_DT,
       SP_DETAILS.END_DT,
       SP_DETAILS.ADMIT_ON_OFFER_ACCEPTANCE_CUT_OFF_DATE,
       SP_DETAILS.EARLIEST_ADMISSION_DATE,
       SP_DETAILS.FIRST_APPN_DATE_VIA_EAPPS_DATE,
       SP_DETAILS.LAST_APPN_DATE_VIA_EAPPS_DATE,
       SP_DETAILS.LAST_WITHDRAW_DATE_VIA_EAPPS_DATE,
       SP_DETAILS.EARLIEST_ENROLMENT_VIA_ESTUDENT_DATE,
       SP_DETAILS.FIRST_EXPANSION_DATE,
       SP_DETAILS.LAST_WITHDRAWAL_WITHOUT_FAIL_DATE,
       SP_DETAILS.LAST_ADMISSION_DATE,
       SP_DETAILS.LAST_APPLICATION_DATE,
       SP_DETAILS.LAST_ENROLMENT_DATE,
       SP_DETAILS.LAST_DATE_FOR_RESULT_ENTRY_DATE,
       SP_DETAILS.LAST_WITHDRAWAL_DATE,
       SP_DETAILS.RESULT_PUBLICATION_DATE,
       SP_DETAILS.STUDY_PERIOD_START_DATE,
       SP_DETAILS.TEACHING_CENSUS_DATE,
       'N'                                       IS_DELETED
FROM (
         SELECT CYR_LOC_DET.CALDR_YR,
                CYR_LOC_DET.LOCATION_CD,
                CYR_LOC_DET.SPRD_CD,
                CYR_LOC_DET.START_DT,
                CYR_LOC_DET.END_DT,
                CAMPUS_CODE.CODE_DESCR CAMPUS,
                STUDY_PERIOD_CODE.CODE_DESCR STUDY_PERIOD,
                DT_DETAILS.ADMIT_ON_OFFER_ACCEPTANCE_CUT_OFF_DATE,
                DT_DETAILS.EARLIEST_ADMISSION_DATE,
                DT_DETAILS.FIRST_APPN_DATE_VIA_EAPPS_DATE,
                DT_DETAILS.LAST_APPN_DATE_VIA_EAPPS_DATE,
                DT_DETAILS.LAST_WITHDRAW_DATE_VIA_EAPPS_DATE,
                DT_DETAILS.EARLIEST_ENROLMENT_VIA_ESTUDENT_DATE,
                DT_DETAILS.FIRST_EXPANSION_DATE,
                DT_DETAILS.LAST_WITHDRAWAL_WITHOUT_FAIL_DATE,
                DT_DETAILS.LAST_ADMISSION_DATE,
                DT_DETAILS.LAST_APPLICATION_DATE,
                DT_DETAILS.LAST_ENROLMENT_DATE,
                DT_DETAILS.LAST_DATE_FOR_RESULT_ENTRY_DATE,
                DT_DETAILS.LAST_WITHDRAWAL_DATE,
                DT_DETAILS.RESULT_PUBLICATION_DATE,
                DT_DETAILS.STUDY_PERIOD_START_DATE,
                DT_DETAILS.TEACHING_CENSUS_DATE
         FROM ODS.AMIS.S1CYR_LOC_DET CYR_LOC_DET
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE CAMPUS_CODE
                                  ON CAMPUS_CODE.CODE_ID = CYR_LOC_DET.LOCATION_CD
                                      AND CAMPUS_CODE.CODE_TYPE = 'LOCATION'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_PERIOD_CODE
                                  ON STUDY_PERIOD_CODE.CODE_ID = CYR_LOC_DET.SPRD_CD
                                      AND STUDY_PERIOD_CODE.CODE_TYPE = 'SPRD_CD'
                  LEFT JOIN (
             SELECT CYR_LOC_DT.CALDR_YR,
                    CYR_LOC_DT.LOCATION_CD,
                    CYR_LOC_DT.SPRD_CD,

                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = '$AAC', CYR_LOC_DT.START_DT, NULL))      ADMIT_ON_OFFER_ACCEPTANCE_CUT_OFF_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = '$EAD', CYR_LOC_DT.START_DT, NULL))      EARLIEST_ADMISSION_DATE,
                    MAX(
                            IFF(CYR_LOC_DT.DT_TYPE_CD = '$EAFE', CYR_LOC_DT.START_DT, NULL)) FIRST_APPN_DATE_VIA_EAPPS_DATE,
                    MAX(
                            IFF(CYR_LOC_DT.DT_TYPE_CD = '$EALE', CYR_LOC_DT.START_DT, NULL)) LAST_APPN_DATE_VIA_EAPPS_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = '$ESLW', CYR_LOC_DT.END_DT, NULL))       LAST_WITHDRAW_DATE_VIA_EAPPS_DATE,
                    MAX(
                            IFF(CYR_LOC_DT.DT_TYPE_CD = '$ESTU', CYR_LOC_DT.START_DT, NULL)) EARLIEST_ENROLMENT_VIA_ESTUDENT_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = '$FAE', CYR_LOC_DT.START_DT, NULL))      FIRST_EXPANSION_DATE,
                    MAX(
                            IFF(CYR_LOC_DT.DT_TYPE_CD = '$LWWF', CYR_LOC_DT.START_DT, NULL)) LAST_WITHDRAWAL_WITHOUT_FAIL_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = 'LAD', CYR_LOC_DT.START_DT, NULL))       LAST_ADMISSION_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = 'LAP', CYR_LOC_DT.START_DT, NULL))       LAST_APPLICATION_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = 'LEN', CYR_LOC_DT.START_DT, NULL))       LAST_ENROLMENT_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = 'LR', CYR_LOC_DT.START_DT, NULL))        LAST_DATE_FOR_RESULT_ENTRY_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = 'LWD', CYR_LOC_DT.START_DT, NULL))       LAST_WITHDRAWAL_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = 'RP', CYR_LOC_DT.START_DT, NULL))        RESULT_PUBLICATION_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = 'SS', CYR_LOC_DT.START_DT, NULL))        STUDY_PERIOD_START_DATE,
                    MAX(IFF(CYR_LOC_DT.DT_TYPE_CD = 'TC', CYR_LOC_DT.START_DT, NULL))        TEACHING_CENSUS_DATE
             FROM ODS.AMIS.S1CYR_LOC_DT CYR_LOC_DT
                      JOIN ODS.AMIS.S1STC_CODE DT_TYPE_CODE
                           ON DT_TYPE_CODE.CODE_ID = CYR_LOC_DT.DT_TYPE_CD
                               AND DT_TYPE_CODE.CODE_TYPE = 'DT_TYPE_CD'
             GROUP BY CYR_LOC_DT.CALDR_YR,
                      CYR_LOC_DT.LOCATION_CD,
                      CYR_LOC_DT.SPRD_CD
         ) DT_DETAILS
                            ON CYR_LOC_DET.CALDR_YR = DT_DETAILS.CALDR_YR
                                AND CYR_LOC_DET.LOCATION_CD = DT_DETAILS.LOCATION_CD
                                AND CYR_LOC_DET.SPRD_CD = DT_DETAILS.SPRD_CD
     ) SP_DETAILS
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_STUDY_PERIOD_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_STUDY_PERIOD_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_STUDY_PERIOD) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_STUDY_PERIOD_KEY = MD5(IFNULL(SP_DETAILS.CALDR_YR, 0) || ',' ||
                                           IFNULL(SP_DETAILS.LOCATION_CD, '') || ',' ||
                                           IFNULL(SP_DETAILS.SPRD_CD, ''))
          AND S.HASH_MD5 = MD5(IFNULL(SP_DETAILS.CALDR_YR, 0) || ',' ||
                               IFNULL(SP_DETAILS.LOCATION_CD, '') || ',' ||
                               IFNULL(SP_DETAILS.SPRD_CD, '') || ',' ||
                               IFNULL(SP_DETAILS.CAMPUS, '') || ',' ||
                               IFNULL(SP_DETAILS.STUDY_PERIOD, '') || ',' ||
                               IFNULL(SP_DETAILS.START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.ADMIT_ON_OFFER_ACCEPTANCE_CUT_OFF_DATE,
                                      TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.EARLIEST_ADMISSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.FIRST_APPN_DATE_VIA_EAPPS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(SP_DETAILS.LAST_APPN_DATE_VIA_EAPPS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(SP_DETAILS.LAST_WITHDRAW_DATE_VIA_EAPPS_DATE,
                                      TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.EARLIEST_ENROLMENT_VIA_ESTUDENT_DATE,
                                      TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.FIRST_EXPANSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.LAST_WITHDRAWAL_WITHOUT_FAIL_DATE,
                                      TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.LAST_ADMISSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.LAST_APPLICATION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.LAST_ENROLMENT_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.LAST_DATE_FOR_RESULT_ENTRY_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) ||
                               ',' ||
                               IFNULL(SP_DETAILS.LAST_WITHDRAWAL_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.RESULT_PUBLICATION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.STUDY_PERIOD_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SP_DETAILS.TEACHING_CENSUS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL('N', '')
            )
    )
;