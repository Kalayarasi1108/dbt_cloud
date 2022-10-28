-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_UAC_OFFER_SUM (SAT_UAC_OFFER_SUM_SK, HUB_UAC_OFFER_KEY, SOURCE, LOAD_DTS, ETL_JOB_ID,
                                               HASH_MD5, COURSE_CODE, COURSE_NAME, COURSE_DESCRIPTION, INSTITUTION_CODE,
                                               INSTITUTION_NAME, IS_MQ_COURSE, STUDENT_REFERENCE_NUMBER,
                                               PREFERENCE_NUMBER, OFFER_DATE, COURSE_CATEGORY_CODE,
                                               COURSE_CATEGORY_DESCRIPTION, OFFER_ROUND_NUMBER, PRS_VALUE,
                                               ADMISSION_BASIS_CODE, ADMISSION_BASIS_DESCRIPTION, RESPONSE_CODE,
                                               RESPONSE_DESCRIPTION, RESPONSE_DATE, PREFERENCE_PRS, HALF_YEAR_INDICATOR,
                                               FIELD_OF_EDUCATION_CODE, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL                          SAT_UAC_OFFER_SK,
       S.HUB_UAC_OFFER_KEY,
       S.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       NULL                                      COURSE_CODE,
       NULL                                      COURSE_NAME,
       NULL                                      COURSE_DESCRIPTION,
       NULL                                      INSTITUTION_CODE,
       NULL                                      INSTITUTION_NAME,
       NULL                                      IS_MQ_COURSE,
       NULL                                      STUDENT_REFERENCE_NUMBER,
       NULL                                      PREFERENCE_NUMBER,
       NULL                                      OFFER_DATE,
       NULL                                      COURSE_CATEGORY_CODE,
       NULL                                      COURSE_CATEGORY_DESCRIPTION,
       NULL                                      OFFER_ROUND_NUMBER,
       NULL                                      PRS_VALUE,
       NULL                                      ADMISSION_BASIS_CODE,
       NULL                                      ADMISSION_BASIS_DESCRIPTION,
       NULL                                      RESPONSE_CODE,
       NULL                                      RESPONSE_DESCRIPTION,
       NULL                                      RESPONSE_DATE,
       NULL                                      PREFERENCE_PRS,
       NULL                                      HALF_YEAR_INDICATOR,
       NULL                                      FIELD_OF_EDUCATION_CODE,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB.REFNUM,
                HUB.ROUNDNUM,
                HUB.COURSE,
                HUB.SOURCE,
                HUB.YEAR,
                SAT.HUB_UAC_OFFER_KEY,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_UAC_OFFER_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_UAC_OFFER_SUM SAT
                  JOIN DATA_VAULT.CORE.HUB_UAC_OFFER HUB
                       ON HUB.HUB_UAC_OFFER_KEY = SAT.HUB_UAC_OFFER_KEY
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.UAC.VW_ALL_OFFER OFFER
        WHERE OFFER.REFNUM = S.REFNUM
          AND OFFER.ROUNDNUM = S.ROUNDNUM
          AND OFFER.COURSE = S.COURSE
          AND OFFER.SOURCE = S.SOURCE
          AND OFFER.YEAR = S.YEAR
    )
;