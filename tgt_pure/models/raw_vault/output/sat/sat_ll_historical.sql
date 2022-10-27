INSERT INTO DATA_VAULT.CORE.SAT_LL_HISTORICAL (SAT_LL_HISTORICAL_SK,
                                               HUB_LL_HISTORICAL_KEY,
                                               SOURCE,
                                               LOAD_DTS,
                                               ETL_JOB_ID,
                                               HASH_MD5,
                                               STUDENT_IDENTIFICATION_CODE,
                                               COURSE_CODE,
                                               COURSE_OF_STUDY_COMMENCEMENT_DATE,
                                               ACADEMIC_ORGANISATIONAL_UNIT_CODE,
                                               EQUIVALENT_FULL_TIME_STUDENT_LOAD,
                                               UNIT_OF_STUDY_CODE,
                                               UNIT_OF_STUDY_CENSUS_DATE,
                                               DISCIPLINE_CODE,
                                               MODE_OF_ATTENDANCE_CODE,
                                               POSTCODE_OR_OVERSEAS_COUNTRY_CODE_LOCATION_OF_HIGHER_EDUCATION,
                                               CITIZEN_RESIDENT_INDICATOR,
                                               MAXIMUM_STUDENT_CONTRIBUTION_INDICATOR,
                                               STUDENT_STATUS_CODE,
                                               TOTAL_AMOUNT_CHARGED,
                                               AMOUNT_PAID_UP_FRONT,
                                               LOAN_FEE,
                                               COMMONWEALTH_HIGHER_EDUCATION_STUDENT_SUPPORT_NUMBER,
                                               WORK_EXPERIENCE_IN_INDUSTRY_INDICATOR,
                                               PERMANENT_RESIDENT_ELIGIBILITY_FOR_HELP_ASSISTANCE,
                                               SUMMER_AND_WINTER_SCHOOL_INDICATOR,
                                               UNIT_OF_STUDY_HELP_DEBT,
                                               NA,
                                               LIABILITY_CATEGORY,
                                               COURSE_TYPE_LEVEL,
                                               COURSE_STUDY_PACKAGE_CODE,
                                               UNIT_CAMPUS_LOCATION,
                                               UNIT_MODE_OF_ATTENDANCE,
                                               COURSE_AVAILABILITY_YEAR,
                                               NA2,
                                               FILE_DATE,
                                               REPORTING_YEAR,
                                               IS_DELETED)

SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               AS SAT_LL_HISTORICAL_SK,
       MD5(IFNULL(LL.STUDENT_IDENTIFICATION_CODE, '') || ',' ||
           IFNULL(LL.COURSE_CODE, '') || ',' ||
           IFNULL(LL.UNIT_OF_STUDY_CENSUS_DATE, 0) || ',' ||
           IFNULL(LL.UNIT_OF_STUDY_CODE, '') || ',' ||
           IFNULL(LL.ACADEMIC_ORGANISATIONAL_UNIT_CODE, '')
           )                                     AS HUB_LL_HISTORICAL_KEY,
       'AMIS'                                    AS SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          AS LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS ETL_JOB_ID,
       MD5(IFNULL(LL.STUDENT_IDENTIFICATION_CODE, '') || ',' ||
           IFNULL(LL.COURSE_CODE, '') || ',' ||
           IFNULL(LL.COURSE_OF_STUDY_COMMENCEMENT_DATE, '') || ',' ||
           IFNULL(LL.ACADEMIC_ORGANISATIONAL_UNIT_CODE, '') || ',' ||
           IFNULL(LL.EQUIVALENT_FULL_TIME_STUDENT_LOAD, 0) || ',' ||
           IFNULL(LL.UNIT_OF_STUDY_CODE, '') || ',' ||
           IFNULL(LL.UNIT_OF_STUDY_CENSUS_DATE, '') || ',' ||
           IFNULL(LL.DISCIPLINE_CODE, 0) || ',' ||
           IFNULL(LL.MODE_OF_ATTENDANCE_CODE, 0) || ',' ||
           IFNULL(LL.POSTCODE_OR_OVERSEAS_COUNTRY_CODE_LOCATION_OF_HIGHER_EDUCATION, '') || ',' ||
           IFNULL(LL.CITIZEN_RESIDENT_INDICATOR, 0) || ',' ||
           IFNULL(LL.MAXIMUM_STUDENT_CONTRIBUTION_INDICATOR, 0) || ',' ||
           IFNULL(LL.STUDENT_STATUS_CODE, '') || ',' ||
           IFNULL(LL.TOTAL_AMOUNT_CHARGED, 0) || ',' ||
           IFNULL(LL.AMOUNT_PAID_UP_FRONT, 0) || ',' ||
           IFNULL(LL.LOAN_FEE, 0) || ',' ||
           IFNULL(LL.COMMONWEALTH_HIGHER_EDUCATION_STUDENT_SUPPORT_NUMBER, '') || ',' ||
           IFNULL(LL.WORK_EXPERIENCE_IN_INDUSTRY_INDICATOR, 0) || ',' ||
           IFNULL(LL.PERMANENT_RESIDENT_ELIGIBILITY_FOR_HELP_ASSISTANCE, '') || ',' ||
           IFNULL(LL.SUMMER_AND_WINTER_SCHOOL_INDICATOR, 0) || ',' ||
           IFNULL(LL.UNIT_OF_STUDY_HELP_DEBT, 0) || ',' ||
           IFNULL(LL.NA, '') || ',' ||
           IFNULL(LL.NA2, '') || ',' ||
           IFNULL(LL.FILE_DATE, '1990-01-01') || ',' ||
           IFNULL(LL.REPORTING_YEAR, '0000') || ',' ||
           'N'
           )                                     AS HASH_MD5,
       LL.STUDENT_IDENTIFICATION_CODE,
       LL.COURSE_CODE,
       LL.COURSE_OF_STUDY_COMMENCEMENT_DATE,
       IFNULL(LL.ACADEMIC_ORGANISATIONAL_UNIT_CODE,''),
       LL.EQUIVALENT_FULL_TIME_STUDENT_LOAD,
       LL.UNIT_OF_STUDY_CODE,
       LL.UNIT_OF_STUDY_CENSUS_DATE,
       LL.DISCIPLINE_CODE,
       LL.MODE_OF_ATTENDANCE_CODE,
       LL.POSTCODE_OR_OVERSEAS_COUNTRY_CODE_LOCATION_OF_HIGHER_EDUCATION,
       LL.CITIZEN_RESIDENT_INDICATOR,
       LL.MAXIMUM_STUDENT_CONTRIBUTION_INDICATOR,
       LL.STUDENT_STATUS_CODE,
       LL.TOTAL_AMOUNT_CHARGED,
       LL.AMOUNT_PAID_UP_FRONT,
       LL.LOAN_FEE,
       LL.COMMONWEALTH_HIGHER_EDUCATION_STUDENT_SUPPORT_NUMBER,
       LL.WORK_EXPERIENCE_IN_INDUSTRY_INDICATOR,
       LL.PERMANENT_RESIDENT_ELIGIBILITY_FOR_HELP_ASSISTANCE,
       LL.SUMMER_AND_WINTER_SCHOOL_INDICATOR,
       LL.UNIT_OF_STUDY_HELP_DEBT,
       LL.NA,
       LEFT(LL.NA, 2)                            AS LIABILITY_CATEGORY,--1,2 SPLIT ATTRIBUTES
       SUBSTR(LL.NA, 3, 2)                       AS COURSE_TYPE_LEVEL,--3,4
       SUBSTR(LL.NA, 5, 15)                      AS COURSE_STUDY_PACKAGE_CODE, --5-19
       SUBSTR(LL.NA, 20, 3)                      AS UNIT_CAMPUS_LOCATION, -- 20,21,22
       SUBSTR(LL.NA, 23, 3)                      AS UNIT_MODE_OF_ATTENDANCE, --23,24,25
       SUBSTR(LL.NA, 26, 4)                      AS COURSE_AVAILABILITY_YEAR,--26,27,28,29
       LL.NA2,
       LL.FILE_DATE,
       LL.REPORTING_YEAR ,
       'N'                                       AS IS_DELETED
FROM (SELECT DISTINCT * FROM ODS.EN_LL.STAGING_LL) AS LL
         LEFT JOIN DATA_VAULT.CORE.SAT_LL_HISTORICAL AS TARGET
                   ON LL.STUDENT_IDENTIFICATION_CODE = TARGET.STUDENT_IDENTIFICATION_CODE
                       AND LL.COURSE_CODE = TARGET.COURSE_CODE
                       AND LL.UNIT_OF_STUDY_CENSUS_DATE = TARGET.UNIT_OF_STUDY_CENSUS_DATE
                       AND LL.UNIT_OF_STUDY_CODE = TARGET.UNIT_OF_STUDY_CODE
                       AND IFNULL(LL.ACADEMIC_ORGANISATIONAL_UNIT_CODE,'') = IFNULL(TARGET.ACADEMIC_ORGANISATIONAL_UNIT_CODE,'')
                       AND LL.FILE_DATE = TARGET.FILE_DATE
                       AND LL.REPORTING_YEAR = TARGET.REPORTING_YEAR
WHERE TARGET.HASH_MD5 IS NULL;