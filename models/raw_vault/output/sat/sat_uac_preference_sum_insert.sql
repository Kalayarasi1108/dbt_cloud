-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_UAC_PREFERENCE_SUM (SAT_UAC_PREFERENCE_SUM_SK, HUB_UAC_PREFERENCE_KEY, SOURCE, LOAD_DTS,
                                                    ETL_JOB_ID, HASH_MD5, ORIGIN_DATE, ENTRY_DATE,
                                                    PREFERENCE_ALLOCATABLITY_STATUS, PREFERENCE_NUMBER, SET_NUMBER,
                                                    STUDENT_REFERENCE_NUMBER, INSTITUTION_CODE, COURSE_CODE,
                                                    COURSE_TITLE, INSTITUTION_NAME,
                                                    HALF_YEAR_INDICATOR,
                                                    COURSE_DESCRIPTION, CAMPUS_CODE, FOS_CODE, FOS_DESCRIPTION,
                                                    IS_MQ_COURSE, YEAR_OF_APPLICATION, INTAKE_YEAR, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_UAC_PREFERENCE_SUM_SK,
       HUB_UAC_PREFERENCE_KEY,
       S.SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(
                   IFNULL(TO_CHAR(S.ORIGIN_DATE, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(TO_CHAR(S.ENTRY_DATE, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(S.PREFERENCE_ALLOCATABILITY_STATUS, '') || ',' ||
                   IFNULL(S.PREFERENCE_NUMBER, 0) || ',' ||
                   IFNULL(S.SET_NUMBER, 0) || ',' ||
                   IFNULL(S.STUDENT_REFERENCE_NUMBER, 0) || ',' ||
                   IFNULL(S.INSTITUTION_CODE, '') || ',' ||
                   IFNULL(S.COURSE_CODE, '') || ',' ||
                   IFNULL(S.COURSE_TITLE, '') || ',' ||
                   IFNULL(S.INSTITUTION_NAME, '') || ',' ||
                   IFNULL(S.HALF_YEAR_INDICATOR, '') || ',' ||
                   IFNULL(S.COURSE_DESCRIPTION, '') || ',' ||
                   IFNULL(S.CAMPUS_CODE, '') || ',' ||
                   IFNULL(S.FOS_CODE, '') || ',' ||
                   IFNULL(S.FOS_DESCRIPTION, '') || ',' ||
                   IFNULL(S.IS_MQ_COURSE, '') || ',' ||
                   IFNULL(S.YEAR_OF_APPLICATION, '') || ',' ||
                   IFNULL(S.INTAKE_YEAR, '') || ',' ||
                   IFNULL(S.IS_DELETED, '')
           )                                     HASH_MD5,
       S.ORIGIN_DATE,
       S.ENTRY_DATE,
       S.PREFERENCE_ALLOCATABILITY_STATUS,
       S.PREFERENCE_NUMBER,
       S.SET_NUMBER,
       S.STUDENT_REFERENCE_NUMBER,
       S.INSTITUTION_CODE,
       S.COURSE_CODE,
       S.COURSE_TITLE,
       S.INSTITUTION_NAME,
       S.HALF_YEAR_INDICATOR,
       S.COURSE_DESCRIPTION,
       S.CAMPUS_CODE,
       S.FOS_CODE,
       S.FOS_DESCRIPTION,
       S.IS_MQ_COURSE,
       S.YEAR_OF_APPLICATION,
       S.INTAKE_YEAR,
       S.IS_DELETED
FROM (SELECT PREF.REFNUM,
             PREF.SETNUM,
             PREF.PREFNUM,
             PREF.COURSE,
             PREF.YEAR,
             PREF.SOURCE,
             MD5(IFNULL(PREF.PREFNUM, 0) || ',' ||
                 IFNULL(PREF.SETNUM, 0) || ',' ||
                 IFNULL(PREF.REFNUM, 0) || ',' ||
                 IFNULL(PREF.COURSE, 0) || ',' ||
                 PREF.YEAR || ',' ||
                 PREF.SOURCE)                                       HUB_UAC_PREFERENCE_KEY,
             PREF.ORIGDATE::TIMESTAMP_NTZ                           ORIGIN_DATE,
             PREF.ENTDATE::TIMESTAMP_NTZ                            ENTRY_DATE,
             PREF.PASVALUE                                          PREFERENCE_ALLOCATABILITY_STATUS,
             PREF.PREFNUM::NUMBER                                   PREFERENCE_NUMBER,
             PREF.SETNUM::NUMBER                                    SET_NUMBER,
             PREF.REFNUM::NUMBER                                    STUDENT_REFERENCE_NUMBER,
             COURSE.INSTCODE                                        INSTITUTION_CODE,
             PREF.COURSE                                            COURSE_CODE,
             COURSE.CSTITLE                                         COURSE_TITLE,
             CASE
                 WHEN IFNULL(LENGTH(REGEXP_REPLACE(INST.LONGNAM, '\\d')), 0) >
                      IFNULL(LENGTH(REGEXP_REPLACE(INST.ORIGIN, '\\d')), 0)
                     THEN INST.LONGNAM
                 WHEN IFNULL(LENGTH(REGEXP_REPLACE(INST.LONGNAM, '\\d')), 0) <
                      IFNULL(LENGTH(REGEXP_REPLACE(INST.ORIGIN, '\\d')), 0)
                     THEN INST.ORIGIN
                 ELSE 'Unknown'
                 END                                                INSTITUTION_NAME,
             CASE
                 WHEN PREF.COURSE LIKE '301%' AND RIGHT(PREF.COURSE, 1) IN ('3', '4', '5')
                     THEN 'First half year'
                 WHEN PREF.COURSE LIKE '301%' AND RIGHT(PREF.COURSE, 1) IN ('6', '7')
                     THEN 'Second half year'
                 WHEN RIGHT(PREF.COURSE, 1) = '0'
                     THEN 'First half year'
                 ELSE 'Second half year'
                 END                                                HALF_YEAR_INDICATOR,
             COURSE.CSDESC                                          COURSE_DESCRIPTION,
             COURSE.CAMPCODE                                        CAMPUS_CODE,
             COURSE.FOSCD                                           FOS_CODE,
             ''                                                     FOS_DESCRIPTION,
             CASE WHEN COURSE.INSTCODE = 'MQ' THEN 'Y' ELSE 'N' END IS_MQ_COURSE,
             PREF.YEAR                                              YEAR_OF_APPLICATION,
             CASE
                 WHEN (PREF.SOURCE LIKE 'UA%' OR PREF.SOURCE LIKE 'OS%') AND PREF.YEAR <= 2019
                     THEN PREF.YEAR::STRING
                 WHEN (PREF.SOURCE LIKE 'UA%' OR PREF.SOURCE LIKE 'OS%') AND PREF.YEAR >= 2020
                     THEN CASE
                              WHEN PREF.COURSE LIKE '301%' AND RIGHT(PREF.COURSE, 1) IN ('3', '4', '5')
                                  THEN PREF.YEAR::INTEGER::STRING
                              WHEN PREF.COURSE LIKE '301%' AND RIGHT(PREF.COURSE, 1) IN ('6', '7')
                                  THEN (PREF.YEAR::INTEGER - 1)::INTEGER::STRING
                              WHEN RIGHT(PREF.COURSE, 1) = '0'
                                  THEN PREF.YEAR::INTEGER::STRING
                              WHEN RIGHT(PREF.COURSE, 1) = '1'
                                  THEN (PREF.YEAR::INTEGER - 1)::INTEGER::STRING
                              ELSE PREF.YEAR::INTEGER::STRING
                     END
                 ELSE PREF.YEAR::INTEGER::STRING
                 END                                                INTAKE_YEAR,
             'N'                                                    IS_DELETED
      FROM ODS.UAC.VW_ALL_PREF PREF
               LEFT OUTER JOIN ODS.UAC.VW_ALL_COURSE COURSE
                               ON COURSE.COURSE = PREF.COURSE
                                   AND COURSE.SOURCE = PREF.SOURCE
                                   AND COURSE.YEAR = PREF.YEAR
               LEFT OUTER JOIN ODS.UAC.VW_ALL_INST INST
                               ON INST.CODE = COURSE.INSTCODE
                                   AND INST.SOURCE = COURSE.SOURCE
                                   AND INST.YEAR = PREF.YEAR
     ) S
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_UAC_PREFERENCE_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_UAC_PREFERENCE_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_UAC_PREFERENCE_SUM) SAT
        WHERE SAT.EFFECTIVE_END_DTS IS NULL
          AND SAT.HUB_UAC_PREFERENCE_KEY = S.HUB_UAC_PREFERENCE_KEY
          AND SAT.HASH_MD5 = MD5(
                    IFNULL(TO_CHAR(S.ORIGIN_DATE, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(TO_CHAR(S.ENTRY_DATE, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(S.PREFERENCE_ALLOCATABILITY_STATUS, '') || ',' ||
                    IFNULL(S.PREFERENCE_NUMBER, 0) || ',' ||
                    IFNULL(S.SET_NUMBER, 0) || ',' ||
                    IFNULL(S.STUDENT_REFERENCE_NUMBER, 0) || ',' ||
                    IFNULL(S.INSTITUTION_CODE, '') || ',' ||
                    IFNULL(S.COURSE_CODE, '') || ',' ||
                    IFNULL(S.COURSE_TITLE, '') || ',' ||
                    IFNULL(S.INSTITUTION_NAME, '') || ',' ||
                    IFNULL(S.HALF_YEAR_INDICATOR, '') || ',' ||
                    IFNULL(S.COURSE_DESCRIPTION, '') || ',' ||
                    IFNULL(S.CAMPUS_CODE, '') || ',' ||
                    IFNULL(S.FOS_CODE, '') || ',' ||
                    IFNULL(S.FOS_DESCRIPTION, '') || ',' ||
                    IFNULL(S.IS_MQ_COURSE, '') || ',' ||
                    IFNULL(S.YEAR_OF_APPLICATION, '') || ',' ||
                    IFNULL(S.INTAKE_YEAR, '') || ',' ||
                    IFNULL(S.IS_DELETED, '')
            )
    );

