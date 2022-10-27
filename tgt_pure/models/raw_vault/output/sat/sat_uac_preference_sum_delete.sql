-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_UAC_PREFERENCE_SUM (SAT_UAC_PREFERENCE_SUM_SK, HUB_UAC_PREFERENCE_KEY, SOURCE, LOAD_DTS,
                                                    ETL_JOB_ID, HASH_MD5, ORIGIN_DATE, ENTRY_DATE,
                                                    PREFERENCE_ALLOCATABLITY_STATUS, PREFERENCE_NUMBER, SET_NUMBER,
                                                    STUDENT_REFERENCE_NUMBER, INSTITUTION_CODE, COURSE_CODE,
                                                    COURSE_TITLE, INSTITUTION_NAME, HALF_YEAR_INDICATOR,
                                                    COURSE_DESCRIPTION, CAMPUS_CODE, FOS_CODE, FOS_DESCRIPTION,
                                                    IS_MQ_COURSE, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL                          SAT_UAC_APPLICANT_SK,
       S.HUB_UAC_PREFERENCE_KEY,
       S.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       NULL                                      ORIGIN_DATE,
       NULL                                      ENTRY_DATE,
       NULL                                      PREFERENCE_ALLOCATABILITY_STATUS,
       NULL                                      PREFERENCE_NUMBER,
       NULL                                      SET_NUMBER,
       NULL                                      STUDENT_REFERENCE_NUMBER,
       NULL                                      INSTITUTION_CODE,
       NULL                                      COURSE_CODE,
       NULL                                      COURSE_TITLE,
       NULL                                      INSTITUTION_NAME,
       NULL                                      HALF_YEAR_INDICATOR,
       NULL                                      COURSE_DESCRIPTION,
       NULL                                      CAMPUS_CODE,
       NULL                                      FOS_CODE,
       NULL                                      FOS_DESCRIPTION,
       NULL                                      IS_MQ_COURSE,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB.REFNUM,
                HUB.PREFNUM,
                HUB.SETNUM,
                HUB.COURSE,
                HUB.SOURCE,
                HUB.YEAR,
                SAT.HUB_UAC_PREFERENCE_KEY,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_UAC_PREFERENCE_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_UAC_PREFERENCE_SUM SAT
                  JOIN DATA_VAULT.CORE.HUB_UAC_PREFERENCE HUB
                       ON HUB.HUB_UAC_PREFERENCE_KEY = SAT.HUB_UAC_PREFERENCE_KEY
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.UAC.VW_ALL_PREF PREF
        WHERE PREF.REFNUM = S.REFNUM
          AND PREF.SETNUM = S.SETNUM
          AND PREF.COURSE = S.COURSE
          AND PREF.PREFNUM = S.PREFNUM
          AND PREF.SOURCE = S.SOURCE
          AND PREF.YEAR = S.YEAR
    )
;