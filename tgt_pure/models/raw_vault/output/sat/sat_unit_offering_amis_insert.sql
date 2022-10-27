INSERT INTO DATA_VAULT.CORE.SAT_UNIT_OFFERING_AMIS (SAT_UNIT_OFFERING_AMIS_SK,
                                                    HUB_UNIT_OFFERING_KEY,
                                                    SOURCE,
                                                    LOAD_DTS,
                                                    ETL_JOB_ID,
                                                    HASH_MD5,
                                                    SPK_CD,
                                                    SPK_VER_NO,
                                                    AVAIL_KEY_NO,
                                                    AVAIL_YR,
                                                    SPRD_CD,
                                                    LOCATION_CD,
                                                    SPK_NO,
                                                    SPK_CAT_CD,
                                                    SPK_CAT_TYPE_CD,
                                                    SPK_STAGE_CD,
                                                    STUDY_PERIOD,
                                                    CAMPUS,
                                                    SPK_CAT_TYPE,
                                                    SPK_STAGE,
                                                    SPK_CAT,
                                                    YEAR_LVL,
                                                    SPK_FULL_TITLE,
                                                    SPK_SHORT_TITLE,
                                                    SPK_ABBR_TITLE,
                                                    EFFCT_DT,
                                                    DISCNT_DATE,
                                                    AVAIL_NO,
                                                    AVAIL_DESC,
                                                    START_DT,
                                                    END_DT,
                                                    AVAIL_STU_FG,
                                                    CONT_AVAIL_FG,
                                                    CURR_NO_ENROLLED,
                                                    OWNING_ORG_UNIT_CD,
                                                    OWNING_ORG_UNIT_NAME,
                                                    TEACHING_ORG_CODE_1,
                                                    TEACHING_ORG_UNIT_NAME_1,
                                                    TEACHING_PERCENTAGE_1,
                                                    TEACHING_ORG_CODE_2,
                                                    TEACHING_ORG_UNIT_NAME_2,
                                                    TEACHING_PERCENTAGE_2,
                                                    TEACHING_ORG_CODE_3,
                                                    TEACHING_ORG_UNIT_NAME_3,
                                                    TEACHING_PERCENTAGE_3,
                                                    CREDIT_MEASURE_VALUE_$CP,
                                                    CREDIT_MEASURE_VALUE_CP2,
                                                    TEACHING_CENSUS_DATE,
                                                    STUDY_PERIOD_START_DATE,
                                                    LAST_DATE_FOR_RESULT_ENTRY_DATE,
                                                    RESULT_PUBLICATION_DATE,
                                                    LAST_WITHDRAWAL_DATE,
                                                    LAST_ENROLMENT_DATE,
                                                    LAST_WITHDRAWAL_WITHOUT_FAIL_DATE,
                                                    LAST_ADMISSION_DATE,
                                                    LAST_APPLICATION_DATE,
                                                    ACADEMIC_SENATE_SPECIAL_MEETING_DATE,
                                                    STUDY_PERIOD_MID_BREAK_START_DATE,
                                                    STUDY_PERIOD_MID_BREAK_END_DATE,
                                                    FULL_YEAR_FEE_2ND_CENSUS_DATE,
                                                    STUDY_PERIOD_BREAK,
                                                    OWNING_FACULTY_CODE,
                                                    OWNING_FACULTY_NAME,
                                                    FIELD_OF_EDUCATION_6_DIGIT_CODE,
                                                    FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
                                                    FIELD_OF_EDUCATION_4_DIGIT_CODE,
                                                    FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
                                                    FIELD_OF_EDUCATION_2_DIGIT_CODE,
                                                    FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
                                                    IS_DELETED)
WITH ORG_UNIT AS (SELECT ORG_UNIT_CD,
                         ORG_UNIT_NM,
                         EFFCT_DT,
                         IFF(LEAD(EFFCT_DT) OVER (PARTITION BY ORG_UNIT_CD ORDER BY EFFCT_DT) <
                             DATEADD(DAY, 1, EXPIRY_DT),
                             DATEADD(DAY, -1, LEAD(EFFCT_DT) OVER (PARTITION BY ORG_UNIT_CD ORDER BY EFFCT_DT)),
                             EXPIRY_DT) AS EXPIRY_DT
                  FROM ODS.AMIS.S1ORG_UNIT)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL                                                            SAT_UNIT_OFFERING_AMIS_SK,
       MD5(
                   IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                   IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                   IFNULL(UN_AVAIL_DET.AVAIL_KEY_NO, 0) || ',' ||
                   IFNULL(UN_AVAIL_DET.AVAIL_YR, 0) || ',' ||
                   IFNULL(UN_AVAIL_DET.SPRD_CD, '') || ',' ||
                   IFNULL(UN_AVAIL_DET.LOCATION_CD, ''))
                                                                                              HUB_UNIT_OFFERING_KEY,
       'AMIS'                                                                                 SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                                       LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                              ETL_JOB_ID,
       MD5(
                   IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                   IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                   IFNULL(UN_AVAIL_DET.AVAIL_KEY_NO, 0) || ',' ||
                   IFNULL(UN_AVAIL_DET.AVAIL_YR, 0) || ',' ||
                   IFNULL(UN_AVAIL_DET.SPRD_CD, '') || ',' ||
                   IFNULL(UN_AVAIL_DET.LOCATION_CD, '') || ',' ||
                   IFNULL(UN_SPK_DET.SPK_NO, 0) || ',' ||
                   IFNULL(UN_SPK_DET.SPK_CAT_CD, '') || ',' ||
                   IFNULL(UN_SPK_DET.SPK_CAT_TYPE_CD, '') || ',' ||
                   IFNULL(UN_SPK_DET.SPK_STAGE_CD, '') || ',' ||
                   IFNULL(STUDY_PERIOD_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(CAMPUS_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(STUDY_PACK_STAGE_TYPE_CODE.SPK_CAT_TYPE_DESC, '') || ',' ||
                   IFNULL(STUDY_PACK_STAGE_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(STUDY_PACK_CAT_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(UN_SPK_DET.YEAR_LVL, 0) || ',' ||
                   IFNULL(UN_SPK_DET.SPK_FULL_TITLE, '') || ',' ||
                   IFNULL(UN_SPK_DET.SPK_SHORT_TITLE, '') || ',' ||
                   IFNULL(UN_SPK_DET.SPK_ABBR_TITLE, '') || ',' ||
                   IFNULL(UN_SPK_DET.EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UN_SPK_DET.DISCNT_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UN_AVAIL_DET.AVAIL_NO, 0) || ',' ||
                   IFNULL(UN_AVAIL_DET.AVAIL_DESC, '') || ',' ||
                   IFNULL(UN_AVAIL_DET.START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UN_AVAIL_DET.END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UN_AVAIL_DET.AVAIL_STU_FG, '') || ',' ||
                   IFNULL(UN_AVAIL_DET.CONT_AVAIL_FG, '') || ',' ||
                   IFNULL(UN_AVAIL_DET.CURR_NO_ENROLLED, 0) || ',' ||
                   IFNULL(COALESCE(SPK_OWN_AVAIL_ORG.ORG_UNIT_CD, OWNING_ORG.OWNING_ORG_UNIT_CD), '') || ',' ||
                   IFNULL(OWNING_ORG_UNIT_NM.ORG_UNIT_NM, '') || ',' ||
                   IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                              TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_1, TEACHING_ORG_UNIT.TEACHING_ORG_CODE_1), 0) ||
                   ',' ||
                   IFNULL(TEACHING_ORG_UNIT_NM_1.ORG_UNIT_NM, '') || ',' ||
                   IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                              TEACHING_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_1, TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_1),
                          0) || ',' ||
                   IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                              TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_2, TEACHING_ORG_UNIT.TEACHING_ORG_CODE_2), 0) ||
                   ',' ||
                   IFNULL(TEACHING_ORG_UNIT_NM_2.ORG_UNIT_NM, '') || ',' ||
                   IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                              TEACHING_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_2, TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_2),
                          0) || ',' ||
                   IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                              TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_3, TEACHING_ORG_UNIT.TEACHING_ORG_CODE_3), 0) ||
                   ',' ||
                   IFNULL(TEACHING_ORG_UNIT_NM_3.ORG_UNIT_NM, '') || ',' ||
                   IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                              TEACHING_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_3, TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_3),
                          0) || ',' ||
                   IFNULL(STUDY_MEASURE.CREDIT_MEASURE_VALUE_$CP, 0) || ',' ||
                   IFNULL(STUDY_MEASURE.CREDIT_MEASURE_VALUE_CP2, 0) || ',' ||
                   IFNULL(AVAIL_DT.TEACHING_CENSUS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.STUDY_PERIOD_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.LAST_DATE_FOR_RESULT_ENTRY_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.RESULT_PUBLICATION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.LAST_WITHDRAWAL_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.LAST_ENROLMENT_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.LAST_WITHDRAWAL_WITHOUT_FAIL_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.LAST_ADMISSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.LAST_APPLICATION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.ACADEMIC_SENATE_SPECIAL_MEETING_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.STUDY_PERIOD_MID_BREAK_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.STUDY_PERIOD_MID_BREAK_END_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.FULL_YEAR_FEE_2ND_CENSUS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(AVAIL_DT.STUDY_PERIOD_BREAK, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(OWN_ORG_FACULTY.ORG_UNIT_CD, '') || ',' ||
                   IFNULL(OWN_ORG_FACULTY.ORG_UNIT_NM, '') || ',' ||
                   IFNULL(COALESCE(FOE_SIX.CODE, '000000'), '') || ',' ||
                   IFNULL(COALESCE(FOE_SIX.DESCRIPTION,
                                   'Course is not a combined course or is a non-award course or is an OUA unit'), '') ||
                   ',' ||
                   IFNULL(COALESCE(FOE_FOUR.CODE, '0000'), '') || ',' ||
                   IFNULL(COALESCE(FOE_FOUR.DESCRIPTION,
                                   'Course is not a combined course or is a non-award course or is an OUA unit'), '') ||
                   ',' ||
                   IFNULL(COALESCE(FOE_TWO.CODE, '00'), '') || ',' ||
                   IFNULL(COALESCE(FOE_TWO.DESCRIPTION,
                                   'Course is not a combined course or is a non-award course or is an OUA unit'), '') ||
                   ',' ||
                   IFNULL('N', '')
           )                                                                                  HASH_MD5,
       UN_SPK_DET.SPK_CD,
       UN_SPK_DET.SPK_VER_NO,
       UN_AVAIL_DET.AVAIL_KEY_NO,
       UN_AVAIL_DET.AVAIL_YR,
       UN_AVAIL_DET.SPRD_CD,
       UN_AVAIL_DET.LOCATION_CD,
       UN_SPK_DET.SPK_NO,
       UN_SPK_DET.SPK_CAT_CD,
       UN_SPK_DET.SPK_CAT_TYPE_CD,
       UN_SPK_DET.SPK_STAGE_CD,
       STUDY_PERIOD_CODE.CODE_DESCR                                                           STUDY_PERIOD,
       CAMPUS_CODE.CODE_DESCR                                                                 CAMPUS,
       STUDY_PACK_STAGE_TYPE_CODE.SPK_CAT_TYPE_DESC                                           SPK_CAT_TYPE,
       STUDY_PACK_STAGE_CODE.CODE_DESCR                                                       SPK_STAGE,
       STUDY_PACK_CAT_CODE.CODE_DESCR                                                         SPK_CAT,
       UN_SPK_DET.YEAR_LVL,
       UN_SPK_DET.SPK_FULL_TITLE,
       UN_SPK_DET.SPK_SHORT_TITLE,
       UN_SPK_DET.SPK_ABBR_TITLE,
       UN_SPK_DET.EFFCT_DT,
       UN_SPK_DET.DISCNT_DATE,
       UN_AVAIL_DET.AVAIL_NO,
       UN_AVAIL_DET.AVAIL_DESC,
       UN_AVAIL_DET.START_DT,
       UN_AVAIL_DET.END_DT,
       UN_AVAIL_DET.AVAIL_STU_FG,
       UN_AVAIL_DET.CONT_AVAIL_FG,
       UN_AVAIL_DET.CURR_NO_ENROLLED,
       COALESCE(SPK_OWN_AVAIL_ORG.ORG_UNIT_CD, OWNING_ORG.OWNING_ORG_UNIT_CD)                 OWNING_ORG_UNIT_CD,
       OWNING_ORG_UNIT_NM.ORG_UNIT_NM                                                         OWNING_ORG_UNIT_NAME,
       IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_1,
           TEACHING_ORG_UNIT.TEACHING_ORG_CODE_1)                                             TEACHING_ORG_CODE_1,
       TEACHING_ORG_UNIT_NM_1.ORG_UNIT_NM                                                     TEACHING_ORG_UNIT_NAME_1,
       IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, TEACHING_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_1,
           TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_1)                                           TEACHING_PERCENTAGE_1,
       IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_2,
           TEACHING_ORG_UNIT.TEACHING_ORG_CODE_2)                                             TEACHING_ORG_CODE_2,
       TEACHING_ORG_UNIT_NM_2.ORG_UNIT_NM                                                     TEACHING_ORG_UNIT_NAME_2,
       IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, TEACHING_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_2,
           TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_2)                                           TEACHING_PERCENTAGE_2,
       IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_3,
           TEACHING_ORG_UNIT.TEACHING_ORG_CODE_3)                                             TEACHING_ORG_CODE_3,
       TEACHING_ORG_UNIT_NM_3.ORG_UNIT_NM                                                     TEACHING_ORG_UNIT_NAME_3,
       IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, TEACHING_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_3,
           TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_3)                                           TEACHING_PERCENTAGE_3,
       STUDY_MEASURE.CREDIT_MEASURE_VALUE_$CP,
       STUDY_MEASURE.CREDIT_MEASURE_VALUE_CP2,
       AVAIL_DT.TEACHING_CENSUS_DATE,
       AVAIL_DT.STUDY_PERIOD_START_DATE,
       AVAIL_DT.LAST_DATE_FOR_RESULT_ENTRY_DATE,
       AVAIL_DT.RESULT_PUBLICATION_DATE,
       AVAIL_DT.LAST_WITHDRAWAL_DATE,
       AVAIL_DT.LAST_ENROLMENT_DATE,
       AVAIL_DT.LAST_WITHDRAWAL_WITHOUT_FAIL_DATE,
       AVAIL_DT.LAST_ADMISSION_DATE,
       AVAIL_DT.LAST_APPLICATION_DATE,
       AVAIL_DT.ACADEMIC_SENATE_SPECIAL_MEETING_DATE,
       AVAIL_DT.STUDY_PERIOD_MID_BREAK_START_DATE,
       AVAIL_DT.STUDY_PERIOD_MID_BREAK_END_DATE,
       AVAIL_DT.FULL_YEAR_FEE_2ND_CENSUS_DATE,
       AVAIL_DT.STUDY_PERIOD_BREAK,
       OWN_ORG_FACULTY.ORG_UNIT_CD                                                            OWNING_FACULTY_CODE,
       OWN_ORG_FACULTY.ORG_UNIT_NM                                                            OWNING_FACULTY_NAME,
       COALESCE(FOE_SIX.CODE, '000000')                                                       FIELD_OF_EDUCATION_6_DIGIT_CODE,
       COALESCE(FOE_SIX.DESCRIPTION,
                'Course is not a combined course or is a non-award course or is an OUA unit') FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
       COALESCE(FOE_FOUR.CODE, '0000')                                                        FIELD_OF_EDUCATION_4_DIGIT_CODE,
       COALESCE(FOE_FOUR.DESCRIPTION,
                'Course is not a combined course or is a non-award course or is an OUA unit') FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
       COALESCE(FOE_TWO.CODE, '00')                                                           FIELD_OF_EDUCATION_2_DIGIT_CODE,
       COALESCE(FOE_TWO.DESCRIPTION,
                'Course is not a combined course or is a non-award course or is an OUA unit') FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
       'N'                                                                                    IS_DELETED
FROM ODS.AMIS.S1SPK_DET UN_SPK_DET
         JOIN ODS.AMIS.S1SPK_AVAIL_DET UN_AVAIL_DET
              ON UN_AVAIL_DET.SPK_NO = UN_SPK_DET.SPK_NO AND UN_AVAIL_DET.SPK_VER_NO = UN_SPK_DET.SPK_VER_NO
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE CAMPUS_CODE
                         ON CAMPUS_CODE.CODE_ID = UN_AVAIL_DET.LOCATION_CD
                             AND CAMPUS_CODE.CODE_TYPE = 'LOCATION'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_PERIOD_CODE
                         ON STUDY_PERIOD_CODE.CODE_ID = UN_AVAIL_DET.SPRD_CD
                             AND STUDY_PERIOD_CODE.CODE_TYPE = 'SPRD_CD'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_PACK_CAT_CODE
                         ON STUDY_PACK_CAT_CODE.CODE_ID = UN_SPK_DET.SPK_CAT_CD
                             AND STUDY_PACK_CAT_CODE.CODE_TYPE = 'SPK_CAT_CD'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_PACK_STAGE_CODE
                         ON STUDY_PACK_STAGE_CODE.CODE_ID = UN_SPK_DET.SPK_STAGE_CD
                             AND STUDY_PACK_STAGE_CODE.CODE_TYPE = 'SPK_STAGE_CD'
         LEFT OUTER JOIN ODS.AMIS.S1CAT_TYPE STUDY_PACK_STAGE_TYPE_CODE
                         ON STUDY_PACK_STAGE_TYPE_CODE.SPK_CAT_CD = UN_SPK_DET.SPK_CAT_CD
                             AND STUDY_PACK_STAGE_TYPE_CODE.SPK_CAT_TYPE_CD = UN_SPK_DET.SPK_CAT_TYPE_CD
         LEFT OUTER JOIN (SELECT SPK_NO, SPK_VER_NO, MAX(ORG_UNIT_CD) OWNING_ORG_UNIT_CD
                          FROM ODS.AMIS.S1SPK_ORG_UNIT OWNING_ORG_UNIT
                          WHERE RESP_CAT_CD = 'O'
                          GROUP BY SPK_NO, SPK_VER_NO) OWNING_ORG
                         ON OWNING_ORG.SPK_NO = UN_SPK_DET.SPK_NO AND OWNING_ORG.SPK_VER_NO = UN_SPK_DET.SPK_VER_NO
         LEFT OUTER JOIN (SELECT AVAIL_KEY_NO, MAX(ORG_UNIT_CD) ORG_UNIT_CD
                          FROM ODS.AMIS.S1SPK_AVAIL_ORG
                          WHERE RESP_CAT_CD = 'O'
                          GROUP BY AVAIL_KEY_NO) SPK_OWN_AVAIL_ORG
                         ON SPK_OWN_AVAIL_ORG.AVAIL_KEY_NO = UN_AVAIL_DET.AVAIL_KEY_NO
         LEFT OUTER JOIN (SELECT A.SPK_NO,
                                 A.SPK_VER_NO,
                                 MAX(IFF(A.RN = 1, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_1,
                                 MAX(IFF(A.RN = 1, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_1,
                                 MAX(IFF(A.RN = 2, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_2,
                                 MAX(IFF(A.RN = 2, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_2,
                                 MAX(IFF(A.RN = 3, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_3,
                                 MAX(IFF(A.RN = 3, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_3
                          FROM (SELECT SPK_NO,
                                       SPK_VER_NO,
                                       ORG_UNIT_CD,
                                       PCENT_RESP,
                                       ROW_NUMBER()
                                               OVER (PARTITION BY SPK_NO, SPK_VER_NO ORDER BY ORG_UNIT_RESP_NO) RN
                                FROM ODS.AMIS.S1SPK_ORG_UNIT OWNING_ORG_UNIT
                                WHERE RESP_CAT_CD = 'T') A
                          GROUP BY A.SPK_NO, A.SPK_VER_NO) TEACHING_ORG_UNIT
                         ON TEACHING_ORG_UNIT.SPK_NO = UN_SPK_DET.SPK_NO AND
                            TEACHING_ORG_UNIT.SPK_VER_NO = UN_SPK_DET.SPK_VER_NO
         LEFT OUTER JOIN (SELECT A.AVAIL_KEY_NO,
                                 MAX(IFF(A.RN = 1, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_1,
                                 MAX(IFF(A.RN = 1, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_1,
                                 MAX(IFF(A.RN = 2, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_2,
                                 MAX(IFF(A.RN = 2, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_2,
                                 MAX(IFF(A.RN = 3, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_3,
                                 MAX(IFF(A.RN = 3, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_3
                          FROM (SELECT AVAIL_KEY_NO,
                                       ORG_UNIT_CD,
                                       PCENT_RESP,
                                       ROW_NUMBER()
                                               OVER (PARTITION BY AVAIL_KEY_NO ORDER BY ORG_UNIT_RESP_NO ) RN
                                FROM ODS.AMIS.S1SPK_AVAIL_ORG T_AVAIL_ORG_UNIT
                                WHERE RESP_CAT_CD = 'T') A
                          GROUP BY A.AVAIL_KEY_NO) TEACHING_AVAIL_ORG_UNIT
                         ON TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO = UN_AVAIL_DET.AVAIL_KEY_NO
         LEFT OUTER JOIN ORG_UNIT OWNING_ORG_UNIT_NM
                         ON OWNING_ORG_UNIT_NM.ORG_UNIT_CD =
                            COALESCE(SPK_OWN_AVAIL_ORG.ORG_UNIT_CD, OWNING_ORG.OWNING_ORG_UNIT_CD)
                             AND UN_AVAIL_DET.START_DT >= OWNING_ORG_UNIT_NM.EFFCT_DT
                             AND UN_AVAIL_DET.START_DT <=
                                 IFF(OWNING_ORG_UNIT_NM.EXPIRY_DT = TO_DATE('1900-01-01', 'YYYY-MM-DD'),
                                     TO_DATE('2050', 'YYYY'), OWNING_ORG_UNIT_NM.EXPIRY_DT)
         LEFT OUTER JOIN ORG_UNIT TEACHING_ORG_UNIT_NM_1
                         ON TEACHING_ORG_UNIT_NM_1.ORG_UNIT_CD =
                            COALESCE(TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_1, TEACHING_ORG_UNIT.TEACHING_ORG_CODE_1)
                             AND UN_AVAIL_DET.START_DT >= TEACHING_ORG_UNIT_NM_1.EFFCT_DT
                             AND UN_AVAIL_DET.START_DT <=
                                 IFF(TEACHING_ORG_UNIT_NM_1.EXPIRY_DT = TO_DATE('1900-01-01', 'YYYY-MM-DD'),
                                     TO_DATE('2050', 'YYYY'), TEACHING_ORG_UNIT_NM_1.EXPIRY_DT)
         LEFT OUTER JOIN ORG_UNIT TEACHING_ORG_UNIT_NM_2
                         ON TEACHING_ORG_UNIT_NM_2.ORG_UNIT_CD =
                            COALESCE(TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_2, TEACHING_ORG_UNIT.TEACHING_ORG_CODE_2)
                             AND UN_AVAIL_DET.START_DT >= TEACHING_ORG_UNIT_NM_2.EFFCT_DT
                             AND UN_AVAIL_DET.START_DT <=
                                 IFF(TEACHING_ORG_UNIT_NM_2.EXPIRY_DT = TO_DATE('1900-01-01', 'YYYY-MM-DD'),
                                     TO_DATE('2050', 'YYYY'), TEACHING_ORG_UNIT_NM_2.EXPIRY_DT)
         LEFT OUTER JOIN ORG_UNIT TEACHING_ORG_UNIT_NM_3
                         ON TEACHING_ORG_UNIT_NM_3.ORG_UNIT_CD =
                            COALESCE(TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_3, TEACHING_ORG_UNIT.TEACHING_ORG_CODE_3)
                             AND UN_AVAIL_DET.START_DT >= TEACHING_ORG_UNIT_NM_3.EFFCT_DT
                             AND UN_AVAIL_DET.START_DT <=
                                 IFF(TEACHING_ORG_UNIT_NM_3.EXPIRY_DT = TO_DATE('1900-01-01', 'YYYY-MM-DD'),
                                     TO_DATE('2050', 'YYYY'), TEACHING_ORG_UNIT_NM_3.EXPIRY_DT)
         LEFT OUTER JOIN (SELECT SPK_NO,
                                 SPK_VER_NO,
                                 MAX(IFF(STUDY_MEASURE_CD = '$CP', STUDY_MEASURE_VAL, NULL)) CREDIT_MEASURE_VALUE_$CP,
                                 MAX(IFF(STUDY_MEASURE_CD = 'CP2', STUDY_MEASURE_VAL, NULL)) CREDIT_MEASURE_VALUE_CP2
                          FROM ODS.AMIS.S1SPK_STUDY_MEASURE
                          GROUP BY SPK_NO, SPK_VER_NO) STUDY_MEASURE
                         ON STUDY_MEASURE.SPK_NO = UN_SPK_DET.SPK_NO AND
                            STUDY_MEASURE.SPK_VER_NO = UN_SPK_DET.SPK_VER_NO
         LEFT OUTER JOIN (SELECT AVAIL_DT.AVAIL_KEY_NO,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'TC', AVAIL_DT.START_DT, NULL))    TEACHING_CENSUS_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'SS', AVAIL_DT.START_DT, NULL))    STUDY_PERIOD_START_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'LR', AVAIL_DT.START_DT, NULL))    LAST_DATE_FOR_RESULT_ENTRY_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'RP', AVAIL_DT.START_DT, NULL))    RESULT_PUBLICATION_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'LWD', AVAIL_DT.START_DT, NULL))   LAST_WITHDRAWAL_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'LEN', AVAIL_DT.START_DT, NULL))   LAST_ENROLMENT_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = '$LWWF', AVAIL_DT.START_DT, NULL)) LAST_WITHDRAWAL_WITHOUT_FAIL_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'LAD', AVAIL_DT.START_DT, NULL))   LAST_ADMISSION_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'LAP', AVAIL_DT.START_DT, NULL))   LAST_APPLICATION_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'ACGEN', AVAIL_DT.START_DT, NULL)) ACADEMIC_SENATE_SPECIAL_MEETING_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'SPMID', AVAIL_DT.START_DT, NULL)) STUDY_PERIOD_MID_BREAK_START_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'SPMID', AVAIL_DT.END_DT, NULL))   STUDY_PERIOD_MID_BREAK_END_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'FEE2', AVAIL_DT.START_DT, NULL))  FULL_YEAR_FEE_2ND_CENSUS_DATE,
                                 MAX(IFF(AVAIL_DT.DT_TYPE_CD = 'SPBRK', AVAIL_DT.START_DT, NULL)) STUDY_PERIOD_BREAK
                          FROM ODS.AMIS.S1SPK_AVAIL_DT AVAIL_DT
                                   JOIN ODS.AMIS.S1SPK_AVAIL_DET AVAIL_DET
                                        ON AVAIL_DET.AVAIL_KEY_NO = AVAIL_DT.AVAIL_KEY_NO
                                   JOIN ODS.AMIS.S1SPK_DET SPK_DET
                                        ON SPK_DET.SPK_CAT_CD = 'UN' AND SPK_DET.SPK_NO = AVAIL_DET.SPK_NO AND
                                           SPK_DET.SPK_VER_NO = AVAIL_DET.SPK_VER_NO
                          GROUP BY AVAIL_DT.AVAIL_KEY_NO) AVAIL_DT
                         ON AVAIL_DT.AVAIL_KEY_NO = UN_AVAIL_DET.AVAIL_KEY_NO
         LEFT OUTER JOIN (SELECT A.ORG_UNIT_CD,
                                 A.ORG_UNIT_SHORT_NM,
                                 A.ORG_UNIT_TYPE_CD,
                                 A.ORG_UNIT_TYPE,
                                 A.ORG_UNIT_NM
                          FROM (
                                   SELECT ORG_UNIT.ORG_UNIT_CD,
                                          ORG_UNIT.EFFCT_DT,
                                          ORG_UNIT.EXPIRY_DT,
                                          ORG_UNIT.ORG_UNIT_SHORT_NM,
                                          ORG_UNIT.ORG_UNIT_TYPE_CD,
                                          ORG_UNIT.ORG_UNIT_NM,
                                          ORG_TYPE_CODE.CODE_DESCR                                                         ORG_UNIT_TYPE,
                                          ROW_NUMBER()
                                                  OVER (PARTITION BY ORG_UNIT.ORG_UNIT_CD ORDER BY ORG_UNIT.EFFCT_DT DESC) RN
                                   FROM ODS.AMIS.S1ORG_UNIT ORG_UNIT
                                            LEFT OUTER JOIN ODS.AMIS.S1STC_CODE ORG_TYPE_CODE
                                                            ON ORG_TYPE_CODE.CODE_ID = ORG_UNIT.ORG_UNIT_TYPE_CD
                                                                AND ORG_TYPE_CODE.CODE_TYPE = 'ORG_TYPE_CD'
                               ) A
                          WHERE A.RN = 1) OWN_ORG_FACULTY
                         ON OWN_ORG_FACULTY.ORG_UNIT_CD = CASE
                                                              WHEN SUBSTR(COALESCE(SPK_OWN_AVAIL_ORG.ORG_UNIT_CD,
                                                                                   OWNING_ORG.OWNING_ORG_UNIT_CD), 1,
                                                                          1) = '8'
                                                                  THEN '9011'
                                                              ELSE SUBSTR(COALESCE(SPK_OWN_AVAIL_ORG.ORG_UNIT_CD,
                                                                                   OWNING_ORG.OWNING_ORG_UNIT_CD), 1,
                                                                          1) || '011'
                             END
         LEFT OUTER JOIN (SELECT A.SPK_NO,
                                 A.SPK_VER_NO,
                                 MAX(IFF(A.PRIM_SECONDARY_CD = 'P', A.FOE_TRLN_CD, NULL)) PRIMARY_FIELD_OF_EDUCATION_CODE,
                                 MAX(IFF(A.PRIM_SECONDARY_CD = 'P', A.FOE_DESC, NULL))    PRIMARY_FIELD_OF_EDUCATION_DESC,
                                 MAX(IFF(A.PRIM_SECONDARY_CD = 'S', A.FOE_TRLN_CD, NULL)) SECONDARY_FIELD_OF_EDUCATION_CODE,
                                 MAX(IFF(A.PRIM_SECONDARY_CD = 'S', A.FOE_DESC, NULL))    SECONDARY_FIELD_OF_EDUCATION_DESC
                          FROM (SELECT UN_SPK_DET.SPK_NO,
                                       UN_SPK_DET.SPK_VER_NO,
                                       UN_SPK_FOE.FOE_CD,
                                       UN_SPK_FOE.PRIM_SECONDARY_CD,
                                       FOE_DET.FOE_TRLN_CD,
                                       FOE_DET.FOE_DESC
                                FROM ODS.AMIS.S1SPK_DET UN_SPK_DET
                                         JOIN ODS.AMIS.S1SPK_FOE UN_SPK_FOE
                                              ON UN_SPK_FOE.SPK_NO = UN_SPK_DET.SPK_NO AND
                                                 UN_SPK_FOE.SPK_VER_NO = UN_SPK_DET.SPK_VER_NO
                                         JOIN ODS.AMIS.S1FOE_DET FOE_DET
                                              ON FOE_DET.FOE_CD = UN_SPK_FOE.FOE_CD
                                WHERE UN_SPK_DET.SPK_CAT_CD = 'UN') A
                          GROUP BY A.SPK_NO, A.SPK_VER_NO) FOE
                         ON FOE.SPK_NO = UN_SPK_DET.SPK_NO AND FOE.SPK_VER_NO = UN_SPK_DET.SPK_VER_NO
         LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_SIX
                         ON FOE_SIX.CODE = LPAD(COALESCE(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, '0'), 6, '0')
         LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_FOUR
                         ON FOE_FOUR.CODE =
                            SUBSTR(LPAD(COALESCE(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, '0'), 6, '0'), 1, 4)
         LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_TWO
                         ON FOE_TWO.CODE =
                            SUBSTR(LPAD(COALESCE(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, '0'), 6, '0'), 1, 2)
WHERE UN_SPK_DET.SPK_CAT_CD = 'UN'
  AND NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_UNIT_OFFERING_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_UNIT_OFFERING_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_UNIT_OFFERING_AMIS) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_UNIT_OFFERING_KEY = MD5(
                    IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                    IFNULL(UN_AVAIL_DET.AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(UN_AVAIL_DET.AVAIL_YR, 0) || ',' ||
                    IFNULL(UN_AVAIL_DET.SPRD_CD, '') || ',' ||
                    IFNULL(UN_AVAIL_DET.LOCATION_CD, ''))
          AND S.HASH_MD5 = MD5(
                    IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                    IFNULL(UN_AVAIL_DET.AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(UN_AVAIL_DET.AVAIL_YR, 0) || ',' ||
                    IFNULL(UN_AVAIL_DET.SPRD_CD, '') || ',' ||
                    IFNULL(UN_AVAIL_DET.LOCATION_CD, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_NO, 0) || ',' ||
                    IFNULL(UN_SPK_DET.SPK_CAT_CD, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_CAT_TYPE_CD, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_STAGE_CD, '') || ',' ||
                    IFNULL(STUDY_PERIOD_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(CAMPUS_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(STUDY_PACK_STAGE_TYPE_CODE.SPK_CAT_TYPE_DESC, '') || ',' ||
                    IFNULL(STUDY_PACK_STAGE_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(STUDY_PACK_CAT_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(UN_SPK_DET.YEAR_LVL, 0) || ',' ||
                    IFNULL(UN_SPK_DET.SPK_FULL_TITLE, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_SHORT_TITLE, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_ABBR_TITLE, '') || ',' ||
                    IFNULL(UN_SPK_DET.EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UN_SPK_DET.DISCNT_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UN_AVAIL_DET.AVAIL_NO, 0) || ',' ||
                    IFNULL(UN_AVAIL_DET.AVAIL_DESC, '') || ',' ||
                    IFNULL(UN_AVAIL_DET.START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UN_AVAIL_DET.END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UN_AVAIL_DET.AVAIL_STU_FG, '') || ',' ||
                    IFNULL(UN_AVAIL_DET.CONT_AVAIL_FG, '') || ',' ||
                    IFNULL(UN_AVAIL_DET.CURR_NO_ENROLLED, 0) || ',' ||
                    IFNULL(COALESCE(SPK_OWN_AVAIL_ORG.ORG_UNIT_CD, OWNING_ORG.OWNING_ORG_UNIT_CD), '') || ',' ||
                    IFNULL(OWNING_ORG_UNIT_NM.ORG_UNIT_NM, '') || ',' ||
                    IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                               TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_1, TEACHING_ORG_UNIT.TEACHING_ORG_CODE_1),
                           0) || ',' ||
                    IFNULL(TEACHING_ORG_UNIT_NM_1.ORG_UNIT_NM, '') || ',' ||
                    IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                               TEACHING_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_1, TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_1),
                           0) || ',' ||
                    IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                               TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_2, TEACHING_ORG_UNIT.TEACHING_ORG_CODE_2),
                           0) || ',' ||
                    IFNULL(TEACHING_ORG_UNIT_NM_2.ORG_UNIT_NM, '') || ',' ||
                    IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                               TEACHING_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_2, TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_2),
                           0) || ',' ||
                    IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                               TEACHING_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_3, TEACHING_ORG_UNIT.TEACHING_ORG_CODE_3),
                           0) || ',' ||
                    IFNULL(TEACHING_ORG_UNIT_NM_3.ORG_UNIT_NM, '') || ',' ||
                    IFNULL(IFF(TEACHING_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL,
                               TEACHING_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_3, TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_3),
                           0) || ',' ||
                    IFNULL(STUDY_MEASURE.CREDIT_MEASURE_VALUE_$CP, 0) || ',' ||
                    IFNULL(STUDY_MEASURE.CREDIT_MEASURE_VALUE_CP2, 0) || ',' ||
                    IFNULL(AVAIL_DT.TEACHING_CENSUS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.STUDY_PERIOD_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.LAST_DATE_FOR_RESULT_ENTRY_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.RESULT_PUBLICATION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.LAST_WITHDRAWAL_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.LAST_ENROLMENT_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.LAST_WITHDRAWAL_WITHOUT_FAIL_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.LAST_ADMISSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.LAST_APPLICATION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.ACADEMIC_SENATE_SPECIAL_MEETING_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.STUDY_PERIOD_MID_BREAK_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.STUDY_PERIOD_MID_BREAK_END_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.FULL_YEAR_FEE_2ND_CENSUS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(AVAIL_DT.STUDY_PERIOD_BREAK, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(OWN_ORG_FACULTY.ORG_UNIT_CD, '') || ',' ||
                    IFNULL(OWN_ORG_FACULTY.ORG_UNIT_NM, '') || ',' ||
                    IFNULL(COALESCE(FOE_SIX.CODE, '000000'), '') || ',' ||
                    IFNULL(COALESCE(FOE_SIX.DESCRIPTION,
                                    'Course is not a combined course or is a non-award course or is an OUA unit'),
                           '') ||
                    ',' ||
                    IFNULL(COALESCE(FOE_FOUR.CODE, '0000'), '') || ',' ||
                    IFNULL(COALESCE(FOE_FOUR.DESCRIPTION,
                                    'Course is not a combined course or is a non-award course or is an OUA unit'),
                           '') ||
                    ',' ||
                    IFNULL(COALESCE(FOE_TWO.CODE, '00'), '') || ',' ||
                    IFNULL(COALESCE(FOE_TWO.DESCRIPTION,
                                    'Course is not a combined course or is a non-award course or is an OUA unit'),
                           '') ||
                    ',' ||
                    IFNULL('N', '')
            )
    )
;
