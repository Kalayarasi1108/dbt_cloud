INSERT INTO DATA_VAULT.CORE.SAT_EN_HISTORICAL (SAT_EN_HISTORICAL_SK,
                                               HUB_EN_HISTORICAL_KEY,
                                               SOURCE,
                                               LOAD_DTS,
                                               ETL_JOB_ID,
                                               HASH_MD5,
                                               STUDENT_IDENTIFICATION_CODE,
                                               COURSE_CODE,
                                               DATE_OF_BIRTH,
                                               GENDER_CODE,
                                               ABORIGINAL_AND_TORRES_STRAIT_ISLANDER_CODE,
                                               LOCATION_CODE_OF_TERM_RESIDENCE,
                                               LOCATION_CODE_OF_PERMANENT_HOME_RESIDENCE,
                                               NEW_BASIS_FOR_ADMISSION_TO_CURRENT_COURSE,
                                               TYPE_OF_ATTENDANCE_CODE,
                                               COUNTRY_OF_BIRTH_CODE,
                                               YEAR_OF_ARRIVAL_IN_AUSTRALIA,
                                               LANGUAGE_SPOKEN_AT_HOME_CODE,
                                               CREDIT_OFFERED_VALUE,
                                               CREDIT_STATUS_HIGHER_EDUCATION_PROVIDER_CODE,
                                               TERTIARY_ENTRANCE_SCORE,
                                               DISABILITY,
                                               PREVIOUS_RTS_EFTSL,
                                               SEPARATION_STATUS_OF_HIGHER_DEGREE_RESEARCH_STUDENTS,
                                               COMMENCING_LOCATION_CODE_OF_PERMANENT_HOME_RESIDENCE,
                                               NAME_OF_SUBURB_FOR_THE_STUDENT,
                                               OVERSEAS_STUDENT_FEE,
                                               HIGHEST_EDUCATIONAL_PARTICIPATION_PRIOR_TO_COMMENCEMENT,
                                               SCHOLARSHIP_CODE,
                                               COMMONWEALTH_HIGHER_EDUCATION_STUDENT_SUPPORT_NUMBER,
                                               REPORTING_PERIOD,
                                               CREDIT_USED_VALUE,
                                               DETAILS_OF_PRIOR_STUDY_FOR_WHICH_CREDIT_WAS_OFFERED,
                                               FIELD_OF_EDUCATION_OF_PRIOR_VET_STUDY_FOR_WHICH_CREDIT_WAS_OFFERED,
                                               LEVEL_OF_EDUCATION_OF_PRIOR_VET_STUDY_FOR_WHICH_CREDIT_WAS_OFFERED,
                                               TYPE_OF_PROVIDER_WHERE_VET_STUDY_WAS_UNDERTAKEN,
                                               YEAR_LEFT_SCHOOL,
                                               HIGHEST_EDUCATIONAL_ATTAINMENT_OF_GUARDIAN_1,
                                               HIGHEST_EDUCATIONAL_ATTAINMENT_OF_GUARDIAN_2,
                                               NA,
                                               NA2,
                                               FILE_DATE,
                                               REPORTING_YEAR,
                                               IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               AS SAT_EN_HISTORICAL_SK,
       MD5(IFNULL(EN.STUDENT_IDENTIFICATION_CODE, '') || ',' ||
           IFNULL(EN.COURSE_CODE, '')
           )                                     AS HUB_EN_HISTORICAL_KEY,
       'AMIS'                                    AS SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          AS LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS ETL_JOB_ID,
       MD5(IFNULL(EN.STUDENT_IDENTIFICATION_CODE, '') || ',' ||
           IFNULL(EN.COURSE_CODE, '') || ',' ||
           IFNULL(EN.DATE_OF_BIRTH, 0) || ',' ||
           IFNULL(EN.GENDER_CODE, '') || ',' ||
           IFNULL(EN.ABORIGINAL_AND_TORRES_STRAIT_ISLANDER_CODE, 0) || ',' ||
           IFNULL(EN.LOCATION_CODE_OF_TERM_RESIDENCE, '') || ',' ||
           IFNULL(EN.LOCATION_CODE_OF_PERMANENT_HOME_RESIDENCE, '') || ',' ||
           IFNULL(EN.NEW_BASIS_FOR_ADMISSION_TO_CURRENT_COURSE, 0) || ',' ||
           IFNULL(EN.TYPE_OF_ATTENDANCE_CODE, 0) || ',' ||
           IFNULL(EN.COUNTRY_OF_BIRTH_CODE, '') || ',' ||
           IFNULL(EN.YEAR_OF_ARRIVAL_IN_AUSTRALIA, '') || ',' ||
           IFNULL(EN.LANGUAGE_SPOKEN_AT_HOME_CODE, 0) || ',' ||
           IFNULL(EN.CREDIT_OFFERED_VALUE, 0) || ',' ||
           IFNULL(EN.CREDIT_STATUS_HIGHER_EDUCATION_PROVIDER_CODE, 0) || ',' ||
           IFNULL(EN.TERTIARY_ENTRANCE_SCORE, 0) || ',' ||
           IFNULL(EN.DISABILITY, 0) || ',' ||
           IFNULL(EN.PREVIOUS_RTS_EFTSL, 0) || ',' ||
           IFNULL(EN.SEPARATION_STATUS_OF_HIGHER_DEGREE_RESEARCH_STUDENTS, 0) || ',' ||
           IFNULL(EN.COMMENCING_LOCATION_CODE_OF_PERMANENT_HOME_RESIDENCE, '') || ',' ||
           IFNULL(EN.NAME_OF_SUBURB_FOR_THE_STUDENT, '') || ',' ||
           IFNULL(EN.OVERSEAS_STUDENT_FEE, 0) || ',' ||
           IFNULL(EN.HIGHEST_EDUCATIONAL_PARTICIPATION_PRIOR_TO_COMMENCEMENT, '') || ',' ||
           IFNULL(EN.SCHOLARSHIP_CODE, 0) || ',' ||
           IFNULL(EN.COMMONWEALTH_HIGHER_EDUCATION_STUDENT_SUPPORT_NUMBER, '') || ',' ||
           IFNULL(EN.REPORTING_PERIOD, 0) || ',' ||
           IFNULL(EN.CREDIT_USED_VALUE, 0) || ',' ||
           IFNULL(EN.DETAILS_OF_PRIOR_STUDY_FOR_WHICH_CREDIT_WAS_OFFERED, 0) || ',' ||
           IFNULL(EN.FIELD_OF_EDUCATION_OF_PRIOR_VET_STUDY_FOR_WHICH_CREDIT_WAS_OFFERED, 0) || ',' ||
           IFNULL(EN.LEVEL_OF_EDUCATION_OF_PRIOR_VET_STUDY_FOR_WHICH_CREDIT_WAS_OFFERED, 0) || ',' ||
           IFNULL(EN.TYPE_OF_PROVIDER_WHERE_VET_STUDY_WAS_UNDERTAKEN, 0) || ',' ||
           IFNULL(EN.YEAR_LEFT_SCHOOL, '') || ',' ||
           IFNULL(EN.HIGHEST_EDUCATIONAL_ATTAINMENT_OF_GUARDIAN_1, '') || ',' ||
           IFNULL(EN.HIGHEST_EDUCATIONAL_ATTAINMENT_OF_GUARDIAN_2, '') || ',' ||
           IFNULL(EN.NA, '') || ',' ||
           IFNULL(EN.NA2, '') || ',' ||
           IFNULL(EN.FILE_DATE, '1990-01-01') || ',' ||
           IFNULL(EN.REPORTING_YEAR, '0000') || ',' ||
           'N'
           )                                     AS HASH_MD5,
       EN.STUDENT_IDENTIFICATION_CODE,
       EN.COURSE_CODE,
       EN.DATE_OF_BIRTH,
       EN.GENDER_CODE,
       EN.ABORIGINAL_AND_TORRES_STRAIT_ISLANDER_CODE,
       EN.LOCATION_CODE_OF_TERM_RESIDENCE,
       EN.LOCATION_CODE_OF_PERMANENT_HOME_RESIDENCE,
       EN.NEW_BASIS_FOR_ADMISSION_TO_CURRENT_COURSE,
       EN.TYPE_OF_ATTENDANCE_CODE,
       EN.COUNTRY_OF_BIRTH_CODE,
       EN.YEAR_OF_ARRIVAL_IN_AUSTRALIA,
       EN.LANGUAGE_SPOKEN_AT_HOME_CODE,
       EN.CREDIT_OFFERED_VALUE,
       EN.CREDIT_STATUS_HIGHER_EDUCATION_PROVIDER_CODE,
       EN.TERTIARY_ENTRANCE_SCORE,
       EN.DISABILITY,
       EN.PREVIOUS_RTS_EFTSL,
       EN.SEPARATION_STATUS_OF_HIGHER_DEGREE_RESEARCH_STUDENTS,
       EN.COMMENCING_LOCATION_CODE_OF_PERMANENT_HOME_RESIDENCE,
       EN.NAME_OF_SUBURB_FOR_THE_STUDENT,
       EN.OVERSEAS_STUDENT_FEE,
       EN.HIGHEST_EDUCATIONAL_PARTICIPATION_PRIOR_TO_COMMENCEMENT,
       EN.SCHOLARSHIP_CODE,
       EN.COMMONWEALTH_HIGHER_EDUCATION_STUDENT_SUPPORT_NUMBER,
       EN.REPORTING_PERIOD,
       EN.CREDIT_USED_VALUE,
       EN.DETAILS_OF_PRIOR_STUDY_FOR_WHICH_CREDIT_WAS_OFFERED,
       EN.FIELD_OF_EDUCATION_OF_PRIOR_VET_STUDY_FOR_WHICH_CREDIT_WAS_OFFERED,
       EN.LEVEL_OF_EDUCATION_OF_PRIOR_VET_STUDY_FOR_WHICH_CREDIT_WAS_OFFERED,
       EN.TYPE_OF_PROVIDER_WHERE_VET_STUDY_WAS_UNDERTAKEN,
       EN.YEAR_LEFT_SCHOOL,
       EN.HIGHEST_EDUCATIONAL_ATTAINMENT_OF_GUARDIAN_1,
       EN.HIGHEST_EDUCATIONAL_ATTAINMENT_OF_GUARDIAN_2,
       EN.NA,
       EN.NA2,
       EN.FILE_DATE,
       EN.REPORTING_YEAR,
       'N'                                       AS IS_DELETED
FROM ODS.EN_LL.STAGING_EN AS EN
         LEFT JOIN DATA_VAULT.CORE.SAT_EN_HISTORICAL AS TARGET ON EN.COURSE_CODE = TARGET.COURSE_CODE
    AND EN.STUDENT_IDENTIFICATION_CODE = TARGET.STUDENT_IDENTIFICATION_CODE
    AND EN.FILE_DATE = TARGET.FILE_DATE
    AND EN.REPORTING_YEAR = TARGET.REPORTING_YEAR
WHERE TARGET.HASH_MD5 IS NULL;