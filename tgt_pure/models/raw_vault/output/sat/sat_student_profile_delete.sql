-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_STUDENT_PROFILE (SAT_STUDENT_PROFILE_SK, HUB_STUDENT_KEY, SOURCE, LOAD_DTS, ETL_JOB_ID,
                                                 HASH_MD5, STUDENT_ID, FULL_NAME, FAMILY_NAME, GIVEN_NAME, MIDDLE_NAME,
                                                 PREFERRED_NAME, INITIALS, TITLE, BIRTH_DATE, GENDER, DECEASED,
                                                 ABORIGINAL_TSI, DISABILITY, BIRTH_COUNTRY, RESIDENCY_COUNTRY,
                                                 RESIDENCY_SUBURB, RESIDENCY_STATE, RESIDENCY_POST_CODE, HOME_LANGUAGE,
                                                 CITIZENSHIP,
                                                 PERMANENT_RESIDENT, ONE_ID, CONSOLIDATED, CONSOLIDATED_TO,
                                                 STUDENT_RECORD_CREATION, PARTY_ID, DOMESTIC_OR_INTERNATIONAL,
                                                 LAST_COMPLETED_COURSE, LAST_COURSE_COMPLETION_DATE, ADMITTED_STUDENT,
                                                 ENROLLED_STUDENT, CURRENT_MQ_STAFF, CURRENT_ON_SCHOLARSHIP,
                                                 GUARDIAN_HIGHEST_EDUCATION, FIRST_ADMISSION_YEAR,
                                                 HIGHEST_QUALIFICATION, HIGHEST_QUALIFICATION_YEAR, SES_INDICATOR,
                                                 CURRENT_YEAR_ENROLMENT, YEAR_LEFT_SCHOOL, YEAR_OF_ARRIVAL_IN_AUSTRALIA,
                                                 BIRTH_COUNTRY_CODE, IS_DELETED)
SELECT CORE.SEQ.NEXTVAL                          SAT_STUDENT_PROFILE_SK,
       S.HUB_STUDENT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       NULL                                      STUDENT_ID,
       NULL                                      FULL_NAME,
       NULL                                      FAMILY_NAME,
       NULL                                      GIVEN_NAME,
       NULL                                      MIDDLE_NAME,
       NULL                                      PREFERRED_NAME,
       NULL                                      INITIALS,
       NULL                                      TITLE,
       NULL                                      BIRTH_DATE,
       NULL                                      GENDER,
       NULL                                      DECEASED,
       NULL                                      ABORIGINAL_TSI,
       NULL                                      DISABILITY,
       NULL                                      BIRTH_COUNTRY,
       NULL                                      RESIDENCY_COUNTRY,
       NULL                                      RESIDENCY_SUBURB,
       NULL                                      RESIDENCY_STATE,
       NULL                                      RESIDENCY_POST_CODE,
       NULL                                      HOME_LANGUAGE,
       NULL                                      CITIZENSHIP,
       NULL                                      PERMANENT_RESIDENT,
       NULL                                      ONE_ID,
       NULL                                      CONSOLIDATED,
       NULL                                      CONSOLIDATED_TO,
       NULL                                      STUDENT_RECORD_CREATION,
       NULL                                      PARTY_ID,
       NULL                                      DOMESTIC_OR_INTERNATIONAL,
       NULL                                      LAST_COMPLETED_COURSE,
       NULL                                      LAST_COURSE_COMPLETION_DATE,
       NULL                                      ADMITTED_STUDENT,
       NULL                                      ENROLLED_STUDENT,
       NULL                                      CURRENT_MQ_STAFF,
       NULL                                      CURRENT_ON_SCHOLARSHIP,
       NULL                                      GUARDIAN_HIGHEST_EDUCATION,
       NULL                                      FIRST_ADMISSION_YEAR,
       NULL                                      HIGHEST_QUALIFICATION,
       NULL                                      HIGHEST_QUALIFICATION_YEAR,
       NULL                                      SES_INDICATOR,
       NULL                                      CURRENT_YEAR_ENROLMENT,
       NULL                                      YEAR_LEFT_SCHOOL,
       NULL                                      YEAR_OF_ARRIVAL_IN_AUSTRALIA,
       NULL                                      BIRTH_COUNTRY_CODE,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB.STU_ID,
                SAT.HUB_STUDENT_KEY,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS) OVER (PARTITION BY SAT.HUB_STUDENT_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_STUDENT_PROFILE SAT
                  JOIN DATA_VAULT.CORE.HUB_STUDENT HUB
                       ON HUB.HUB_STUDENT_KEY = SAT.HUB_STUDENT_KEY AND HUB.SOURCE = 'AMIS'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1STU_DET STU_DET
        WHERE STU_DET.STU_ID = S.STU_ID
    )
;
