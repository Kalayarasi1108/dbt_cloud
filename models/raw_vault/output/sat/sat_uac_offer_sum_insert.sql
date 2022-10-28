-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_UAC_OFFER_SUM (SAT_UAC_OFFER_SUM_SK, HUB_UAC_OFFER_KEY, SOURCE, LOAD_DTS, ETL_JOB_ID,
                                               HASH_MD5, COURSE_CODE, COURSE_NAME, COURSE_DESCRIPTION, INSTITUTION_CODE,
                                               INSTITUTION_NAME, IS_MQ_COURSE, STUDENT_REFERENCE_NUMBER,
                                               PREFERENCE_NUMBER, OFFER_DATE, COURSE_CATEGORY_CODE,
                                               COURSE_CATEGORY_DESCRIPTION, OFFER_ROUND_NUMBER, PRS_VALUE,
                                               ADMISSION_BASIS_CODE, ADMISSION_BASIS_DESCRIPTION, RESPONSE_CODE,
                                               RESPONSE_DESCRIPTION, RESPONSE_DATE, PREFERENCE_PRS, HALF_YEAR_INDICATOR,
                                               FIELD_OF_EDUCATION_CODE, ENTRY_SCHEME, YEAR_OF_APPLICATION, INTAKE_YEAR,
                                               IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_UAC_OFFER_SUM_SK,
       S.HUB_UAC_OFFER_KEY,
       S.SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(S.COURSE_CODE, '') || ',' ||
           IFNULL(S.COURSE_NAME, '') || ',' ||
           IFNULL(S.COURSE_DESCRIPTION, '') || ',' ||
           IFNULL(S.INSTITUTION_CODE, '') || ',' ||
           IFNULL(S.INSTITUTION_NAME, '') || ',' ||
           IFNULL(S.IS_MQ_COURSE, '') || ',' ||
           IFNULL(S.STUDENT_REFERENCE_NUMBER, 0) || ',' ||
           IFNULL(S.PREFERENCE_NUMBER, 0) || ',' ||
           IFNULL(TO_CHAR(S.OFFER_DATE, 'YYYY-MM-DD'), '') || ',' ||
           IFNULL(S.COURSE_CATEGORY_CODE, '') || ',' ||
           IFNULL(S.COURSE_CATEGORY_DESCRIPTION, '') || ',' ||
           IFNULL(S.OFFER_ROUND_NUMBER, 0) || ',' ||
           IFNULL(S.PRS_VALUE, '') || ',' ||
           IFNULL(S.ADMISSION_BASIS_CODE, '') || ',' ||
           IFNULL(S.ADMISSION_BASIS_DESCRIPTION, '') || ',' ||
           IFNULL(S.RESPONSE_CODE, '') || ',' ||
           IFNULL(S.RESPONSE_DESCRIPTION, '') || ',' ||
           IFNULL(TO_CHAR(S.RESPONSE_DATE, 'YYYY-MM-DD'), '') || ',' ||
           IFNULL(S.PREFERENCE_PRS, '') || ',' ||
           IFNULL(S.HALF_YEAR_INDICATOR, '') || ',' ||
           IFNULL(S.FIELD_OF_EDUCATION_CODE, '') || ',' ||
           IFNULL(S.ENTRY_SCHEME, '') || ',' ||
           IFNULL(S.YEAR_OF_APPLICATION, '') || ',' ||
           IFNULL(S.INTAKE_YEAR, '') || ',' ||
           IFNULL(S.IS_DELETED, ''))             HASH_MD5,
       S.COURSE_CODE,
       S.COURSE_NAME,
       S.COURSE_DESCRIPTION,
       S.INSTITUTION_CODE,
       S.INSTITUTION_NAME,
       S.IS_MQ_COURSE,
       S.STUDENT_REFERENCE_NUMBER,
       S.PREFERENCE_NUMBER,
       S.OFFER_DATE,
       S.COURSE_CATEGORY_CODE,
       S.COURSE_CATEGORY_DESCRIPTION,
       S.OFFER_ROUND_NUMBER,
       S.PRS_VALUE,
       S.ADMISSION_BASIS_CODE,
       S.ADMISSION_BASIS_DESCRIPTION,
       S.RESPONSE_CODE,
       S.RESPONSE_DESCRIPTION,
       S.RESPONSE_DATE,
       S.PREFERENCE_PRS,
       S.HALF_YEAR_INDICATOR,
       S.FIELD_OF_EDUCATION_CODE,
       S.ENTRY_SCHEME,
       S.YEAR_OF_APPLICATION,
       S.INTAKE_YEAR,
       S.IS_DELETED
FROM (
         SELECT MD5(IFNULL(OFFER.ROUNDNUM, 0) || ',' ||
                    IFNULL(OFFER.REFNUM, 0) || ',' ||
                    IFNULL(OFFER.COURSE, 0) || ',' ||
                    OFFER.YEAR || ',' ||
                    OFFER.SOURCE)                                      HUB_UAC_OFFER_KEY,
                OFFER.YEAR,
                OFFER.SOURCE,
                OFFER.ROUNDNUM,
                OFFER.REFNUM,
                OFFER.COURSE,
                OFFER.COURSE                                           COURSE_CODE,
                COURSE.CSTITLE                                         COURSE_NAME,
                COURSE.CSDESC                                          COURSE_DESCRIPTION,
                COURSE.INSTCODE                                        INSTITUTION_CODE,
                CASE
                    WHEN IFNULL(LENGTH(REGEXP_REPLACE(INST.LONGNAM, '\\d')), 0) >
                         IFNULL(LENGTH(REGEXP_REPLACE(INST.ORIGIN, '\\d')), 0)
                        THEN INST.LONGNAM
                    WHEN IFNULL(LENGTH(REGEXP_REPLACE(INST.LONGNAM, '\\d')), 0) <
                         IFNULL(LENGTH(REGEXP_REPLACE(INST.ORIGIN, '\\d')), 0)
                        THEN INST.ORIGIN
                    ELSE 'Unknown'
                    END                                                INSTITUTION_NAME,
                CASE WHEN COURSE.INSTCODE = 'MQ' THEN 'Y' ELSE 'N' END IS_MQ_COURSE,
                OFFER.REFNUM::NUMBER                                   STUDENT_REFERENCE_NUMBER,
                OFFER.PREFNUM::NUMBER                                  PREFERENCE_NUMBER,
                OFFER.OFFDATE::TIMESTAMP_NTZ                           OFFER_DATE,
                COURSE.CATEGORY                                        COURSE_CATEGORY_CODE,
                CASE
                    WHEN COURSE.CATEGORY = 'H' THEN 'UG domestic category 1 (in-line single offer mode)'
                    WHEN COURSE.CATEGORY = 'F' THEN 'UG domestic category 2 (double offer mode)'
                    ELSE NULL
                    END                                                COURSE_CATEGORY_DESCRIPTION,
                OFFER.ROUNDNUM::NUMBER                                 OFFER_ROUND_NUMBER,
                OFFER.PRSVALUE                                         PRS_VALUE,
                OFFER.ADMBASIS                                         ADMISSION_BASIS_CODE,
                ADMISSION_BASIS_CODE.CODE_DESCR                        ADMISSION_BASIS_DESCRIPTION,
                OFFER.RESPONSE                                         RESPONSE_CODE,
                CASE
                    WHEN OFFER.RESPONSE = 'A' THEN 'Accepted offer'
                    WHEN OFFER.RESPONSE = 'D' THEN 'Deferred offer'
                    WHEN OFFER.RESPONSE = 'R' THEN 'Rejected offer'
                    WHEN OFFER.RESPONSE = 'U' THEN 'No response (yet) recorded'
                    ELSE NULL
                    END                                                RESPONSE_DESCRIPTION,
                OFFER.RESDATE::TIMESTAMP_NTZ                           RESPONSE_DATE,
                OFFER.PREFPRS                                          PREFERENCE_PRS,
                CASE
                    WHEN OFFER.COURSE LIKE '301%' AND RIGHT(OFFER.COURSE, 1) IN ('3', '4', '5')
                        THEN 'First half year'
                    WHEN OFFER.COURSE LIKE '301%' AND RIGHT(OFFER.COURSE, 1) IN ('6', '7')
                        THEN 'Second half year'
                    WHEN RIGHT(OFFER.COURSE, 1) = '0'
                        THEN 'First half year'
                    ELSE 'Second half year'
                    END                                                HALF_YEAR_INDICATOR,
                COURSE.FOSCD                                           FIELD_OF_EDUCATION_CODE,
                CASE
                    WHEN PRS_CODE.CODE IS NOT NULL
                        THEN PRS_CODE.LONG_NAME
                    WHEN PRS_CODE.CODE IS NULL
                        AND REGEXP_SUBSTR(OFFER.PRSVALUE, '([1-9][0-9](\\.\\d+|($|:])))', 1, 1, 's', 1)::FLOAT < 99.99
                        THEN 'ATAR or equivalent scheme'
                    ELSE 'N/A'
                    END                                                ENTRY_SCHEME,
                OFFER.YEAR                                             YEAR_OF_APPLICATION,
                CASE
                    WHEN (OFFER.SOURCE LIKE 'UA%' OR OFFER.SOURCE LIKE 'OS%') AND OFFER.YEAR <= 2019
                        THEN OFFER.YEAR::STRING
                    WHEN (OFFER.SOURCE LIKE 'UA%' OR OFFER.SOURCE LIKE 'OS%') AND OFFER.YEAR >= 2020
                        THEN CASE
                                 WHEN OFFER.COURSE LIKE '301%' AND RIGHT(OFFER.COURSE, 1) IN ('3', '4', '5')
                                     THEN OFFER.YEAR::INTEGER::STRING
                                 WHEN OFFER.COURSE LIKE '301%' AND RIGHT(OFFER.COURSE, 1) IN ('6', '7')
                                     THEN (OFFER.YEAR::INTEGER - 1)::INTEGER::STRING
                                 WHEN RIGHT(OFFER.COURSE, 1) = '0'
                                     THEN OFFER.YEAR::INTEGER::STRING
                                 WHEN RIGHT(OFFER.COURSE, 1) = '1'
                                     THEN (OFFER.YEAR::INTEGER - 1)::INTEGER::STRING
                                 ELSE OFFER.YEAR::INTEGER::STRING
                        END
                    ELSE OFFER.YEAR::STRING
                    END                                                INTAKE_YEAR,
                'N'                                                    IS_DELETED
         FROM ODS.UAC.VW_ALL_OFFER OFFER
                  LEFT OUTER JOIN ODS.UAC.VW_ALL_COURSE COURSE
                                  ON COURSE.COURSE = OFFER.COURSE
                                      AND COURSE.SOURCE = OFFER.SOURCE
                                      AND COURSE.YEAR = OFFER.YEAR
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE ADMISSION_BASIS_CODE
                                  ON ADMISSION_BASIS_CODE.CODE_ID = OFFER.ADMBASIS AND
                                     ADMISSION_BASIS_CODE.CODE_TYPE = 'BASIS_ADMSN_CD'
                  LEFT OUTER JOIN DATA_VAULT.CORE.REF_PRS_VALUE_SPECIAL_OFFER_CODE PRS_CODE
                                  ON CONTAINS(IFF(OFFER.SOURCE LIKE '%PGP%', '155', OFFER.PRSVALUE),
                                              PRS_CODE.CODE)
                  LEFT OUTER JOIN ODS.UAC.VW_ALL_INST INST
                                  ON INST.CODE = COURSE.INSTCODE
                                      AND INST.SOURCE = COURSE.SOURCE
                                      AND INST.YEAR = OFFER.YEAR
     ) S
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_UAC_OFFER_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_UAC_OFFER_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_UAC_OFFER_SUM) SAT
        WHERE SAT.EFFECTIVE_END_DTS IS NULL
          AND SAT.HUB_UAC_OFFER_KEY = S.HUB_UAC_OFFER_KEY
          AND SAT.HASH_MD5 = MD5(IFNULL(S.COURSE_CODE, '') || ',' ||
                                 IFNULL(S.COURSE_NAME, '') || ',' ||
                                 IFNULL(S.COURSE_DESCRIPTION, '') || ',' ||
                                 IFNULL(S.INSTITUTION_CODE, '') || ',' ||
                                 IFNULL(S.INSTITUTION_NAME, '') || ',' ||
                                 IFNULL(S.IS_MQ_COURSE, '') || ',' ||
                                 IFNULL(S.STUDENT_REFERENCE_NUMBER, 0) || ',' ||
                                 IFNULL(S.PREFERENCE_NUMBER, 0) || ',' ||
                                 IFNULL(TO_CHAR(S.OFFER_DATE, 'YYYY-MM-DD'), '') || ',' ||
                                 IFNULL(S.COURSE_CATEGORY_CODE, '') || ',' ||
                                 IFNULL(S.COURSE_CATEGORY_DESCRIPTION, '') || ',' ||
                                 IFNULL(S.OFFER_ROUND_NUMBER, 0) || ',' ||
                                 IFNULL(S.PRS_VALUE, '') || ',' ||
                                 IFNULL(S.ADMISSION_BASIS_CODE, '') || ',' ||
                                 IFNULL(S.ADMISSION_BASIS_DESCRIPTION, '') || ',' ||
                                 IFNULL(S.RESPONSE_CODE, '') || ',' ||
                                 IFNULL(S.RESPONSE_DESCRIPTION, '') || ',' ||
                                 IFNULL(TO_CHAR(S.RESPONSE_DATE, 'YYYY-MM-DD'), '') || ',' ||
                                 IFNULL(S.PREFERENCE_PRS, '') || ',' ||
                                 IFNULL(S.HALF_YEAR_INDICATOR, '') || ',' ||
                                 IFNULL(S.FIELD_OF_EDUCATION_CODE, '') || ',' ||
                                 IFNULL(S.ENTRY_SCHEME, '') || ',' ||
                                 IFNULL(S.YEAR_OF_APPLICATION, '') || ',' ||
                                 IFNULL(S.INTAKE_YEAR, '') || ',' ||
                                 IFNULL(S.IS_DELETED, ''))
    )
;
