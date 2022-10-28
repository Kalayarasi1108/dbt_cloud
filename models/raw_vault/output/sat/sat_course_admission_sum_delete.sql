-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_COURSE_ADMISSION_SUM (SAT_COURSE_ADMISSION_SUM_SK, HUB_COURSE_ADMISSION_KEY, SOURCE,
                                                      LOAD_DTS,
                                                      ETL_JOB_ID, HASH_MD5, STUDENT_ID, PARENT_SSP_NO, ADMISSION_DATE,
                                                      ATTENDANCE_MODE, ATTENDANCE_TYPE, EQUIVALENT_YEAR_LEVEL,
                                                      COURSE_LIABILITY_CATEGORY, CURRENT_LIABILITY_CATEGORY,
                                                      COURSE_ANNUAL_CREDIT_POINTS, COURSE_TOTAL_CREDIT_POINTS,
                                                      ADVANCED_STANDING_CREDIT_POINTS, CREDITED_CREDIT_POINTS,
                                                      COMPLETED_CREDIT_POINTS, ENROLLED_CREDIT_POINTS,
                                                      COURSE_CAT_TYPE_CODE, COURSE_CAT_TYPE, COURSE_CODE,
                                                      COURSE_VERSION, COURSE_FULL_TITLE, CONSUMED_EFTSL,
                                                      COURSE_TOTAL_EFTSL, APPLICATION_PATH, SSP_STAGE, SSP_STATUS,
                                                      CAMPUS, AVAILABILITY_YEAR, COURSE_SPK_NO, AVAIL_KEY_NO, AVAIL_NO,
                                                      STUDY_MODE, STUDY_TYPE, STUDY_BASIS, HDR_COURSE,
                                                      COMMENCING_OR_CONTINUING, COURSE_CAT_TYPE_LEVEL,
                                                      UNIT_COMPLETE_ATTEMPT, UNIT_FAIL_NUMBER, UNIT_PASS_NUMBER,
                                                      REPARENTED_COURSE, COURSE_COMPLETION_DATE, CONFERRAL_DATE,
                                                      CURRENT_WAM, UNIT_ENROLLED, UNIT_ENROLLED_NUMBER,
                                                      GOV_REPORTED_FLAG, CURRENT_YEAR_ENROLMENT,
                                                      CROSS_INSTITUTION_COURSE, COURSE_OWNING_ORG_UNIT_CODE,
                                                      COURSE_OWNING_ORG_UNIT_TYPE, COURSE_OWNING_ORG_UNIT_NAME,
                                                      COURSE_EXPECTED_COMPLETION_DATE, COURSE_START_DATE,
                                                      COURSE_OF_STUDY_START_DATE, COURSE_MUST_COMPLETE_DATE,
                                                      CONSOLIDATED, CURRENT_ATTENDANCE_MODE,
                                                      COURSE_TYPE_CODE, COURSE_TYPE_DESCRIPTION,
                                                      PRIMARY_FIELD_OF_EDUCATION_CODE,
                                                      PRIMARY_FIELD_OF_EDUCATION_DESC,
                                                      SECONDARY_FIELD_OF_EDUCATION_CODE,
                                                      SECONDARY_FIELD_OF_EDUCATION_DESC,
                                                      CRICOS_CODE, FIRST_UNIT_ENROLLED_DATE, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_ADMISSION_DATE_SK,
       S.HUB_COURSE_ADMISSION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       NULL                                      STUDENT_ID,
       NULL                                      PARENT_SSP_NO,
       NULL                                      ADMISSION_DATE,
       NULL                                      ATTENDANCE_MODE,
       NULL                                      ATTENDANCE_TYPE,
       NULL                                      EQUIVALENT_YEAR_LEVEL,
       NULL                                      COURSE_LIABILITY_CATEGORY,
       NULL                                      CURRENT_LIABILITY_CATEGORY,
       NULL                                      COURSE_ANNUAL_CREDIT_POINTS,
       NULL                                      COURSE_TOTAL_CREDIT_POINTS,
       NULL                                      ADVANCED_STANDING_CREDIT_POINTS,
       NULL                                      CREDITED_CREDIT_POINTS,
       NULL                                      COMPLETED_CREDIT_POINTS,
       NULL                                      ENROLLED_CREDIT_POINTS,
       NULL                                      COURSE_CAT_TYPE_CODE,
       NULL                                      COURSE_CAT_TYPE,
       NULL                                      COURSE_CODE,
       NULL                                      COURSE_VERSION,
       NULL                                      COURSE_FULL_TITLE,
       NULL                                      CONSUMED_EFTSL,
       NULL                                      COURSE_TOTAL_EFTSL,
       NULL                                      APPLICATION_PATH,
       NULL                                      SSP_STAGE,
       NULL                                      SSP_STATUS,
       NULL                                      CAMPUS,
       NULL                                      AVAILABILITY_YEAR,
       NULL                                      COURSE_SPK_NO,
       NULL                                      AVAIL_KEY_NO,
       NULL                                      AVAIL_NO,
       NULL                                      STUDY_MODE,
       NULL                                      STUDY_TYPE,
       NULL                                      STUDY_BASIS,
       NULL                                      HDR_COURSE,
       NULL                                      COMMENCING_OR_CONTINUING,
       NULL                                      COURSE_CAT_TYPE_LEVEL,
       NULL                                      UNIT_COMPLETE_ATTEMPT,
       NULL                                      UNIT_FAIL_NUMBER,
       NULL                                      UNIT_PASS_NUMBER,
       NULL                                      REPARENTED_COURSE,
       NULL                                      COURSE_COMPLETION_DATE,
       NULL                                      CONFERRAL_DATE,
       NULL                                      CURRENT_WAM,
       NULL                                      UNIT_ENROLLED,
       NULL                                      UNIT_ENROLLED_NUMBER,
       NULL                                      GOV_REPORTED_FLAG,
       NULL                                      CURRENT_YEAR_ENROLMENT,
       NULL                                      CROSS_INSTITUTION_COURSE,
       NULL                                      COURSE_OWNING_ORG_UNIT_CODE,
       NULL                                      COURSE_OWNING_ORG_UNIT_TYPE,
       NULL                                      COURSE_OWNING_ORG_UNIT_NAME,
       NULL                                      COURSE_EXPECTED_COMPLETION_DATE,
       NULL                                      COURSE_START_DATE,
       NULL                                      COURSE_OF_STUDY_START_DATE,
       NULL                                      COURSE_MUST_COMPLETE_DATE,
       NULL                                      CONSOLIDATED,
       NULL                                      CURRENT_ATTENDANCE_MODE,
       NULL                                      COURSE_TYPE_CODE,
       NULL                                      COURSE_TYPE_DESCRIPTION,
       NULL                                      PRIMARY_FIELD_OF_EDUCATION_CODE,
       NULL                                      PRIMARY_FIELD_OF_EDUCATION_DESC,
       NULL                                      SECONDARY_FIELD_OF_EDUCATION_CODE,
       NULL                                      SECONDARY_FIELD_OF_EDUCATION_DESC,
       NULL                                      CRICOS_CODE,
       NULL                                      FIRST_UNIT_ENROLLED_DATE,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB_COURSE_ADMISSION_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_ADMISSION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_COURSE_ADMISSION_SUM
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                 JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                      ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                          AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
                          AND CS_SPK_DET.SPK_CAT_CD = 'CS'
        WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
          AND S.HUB_COURSE_ADMISSION_KEY = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.SSP_NO, 0)
            )
    )
;