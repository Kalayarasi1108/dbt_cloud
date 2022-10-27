-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_STUDYLINK_APPLICATION_COURSE_FILE (
                                                SAT_STUDYLINK_APPLICATION_COURSE_FILE_SK,
                                                HUB_STUDYLINK_APPLICATION_COURSE_KEY,
                                                APPLICATIONID,
                                                COURSE_CODE,
                                                SOURCE,
                                                LOAD_DTS,
                                                ETL_JOB_ID,
                                                HASH_MD5,
                                                IS_DELETED )
SELECT  DATA_VAULT.CORE.SEQ.NEXTVAL SAT_STUDYLINK_APPLICATION_COURSE_FILE_SK,
        S.HUB_STUDYLINK_APPLICATION_COURSE_KEY,
        S.APPLICATIONID,
        S.COURSE_CODE,
       'STUDYLINK' SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('') HASH_MD5,
       'Y' IS_DELETED
FROM (
         SELECT SAT.HUB_STUDYLINK_APPLICATION_COURSE_KEY,
                HUB.APPLICATIONID,
                HUB.COURSE_CODE,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS) OVER(PARTITION BY SAT.HUB_STUDYLINK_APPLICATION_COURSE_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_STUDYLINK_APPLICATION_COURSE_FILE SAT
                  JOIN DATA_VAULT.CORE.HUB_STUDYLINK_APPLICATION_COURSE HUB
                       ON HUB.HUB_STUDYLINK_APPLICATION_COURSE_KEY = SAT.HUB_STUDYLINK_APPLICATION_COURSE_KEY
                              AND HUB.SOURCE = 'STUDYLINK'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM  (SELECT A.APPLICATIONID,
                DECODE(R.SEQ, 0, A.DEG_S1_CODE,
                       1, A.FND_S1_CODE,
                       2, A.DIP_S1_CODE,
                       3, A.ENG_S1_CODE,
                       4, A.GCGD_S1_CODE,
                       5, A.SAE_S1_CODE)                              COURSE_CODE
         FROM (
                  SELECT APPLICATIONID,
                         ENG_S1_CODE,
                         FND_S1_CODE,
                         DIP_S1_CODE,
                         GCGD_S1_CODE,
                         DEG_S1_CODE,
                         SAE_S1_CODE,
                         RN
                  FROM (
                           SELECT APPLICATIONID,
                                  ENG_S1_CODE,
                                  FND_S1_CODE,
                                  DIP_S1_CODE,
                                  GCGD_S1_CODE,
                                  DEG_S1_CODE,
                                  DEG_INTAKE_DESC,
                                  SAE_S1_CODE,
                                  FILE_DATE,
                                  ROW_NUMBER() OVER (PARTITION BY APPLICATIONID ORDER BY FILE_DATE DESC) RN
                           FROM ODS.STUDYLINK.STUDYLINK_APPLICATION_CSV_EXTRACT
                       )
                  WHERE RN = 1
              ) A
                  JOIN (
             SELECT 0 SEQ
             UNION ALL
             SELECT 1
             UNION ALL
             SELECT 2
             UNION ALL
             SELECT 3
             UNION ALL
             SELECT 4
             UNION ALL
             SELECT 5
         ) R ON 1 = 1
         WHERE DECODE(R.SEQ, 0, DEG_S1_CODE,
                      1, FND_S1_CODE,
                      2, DIP_S1_CODE,
                      3, ENG_S1_CODE,
                      4, GCGD_S1_CODE,
                      5, SAE_S1_CODE) IS NOT NULL) V
        WHERE V.APPLICATIONID = S.APPLICATIONID
           AND V.COURSE_CODE = S.COURSE_CODE
    )
;
