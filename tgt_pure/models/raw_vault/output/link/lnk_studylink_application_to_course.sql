INSERT INTO DATA_VAULT.CORE.LNK_STUDYLINK_APPLICATION_TO_COURSE (LNK_STUDYLINK_APPLICATION_TO_COURSE_KEY,
                                                                 HUB_STUDYLINK_APPLICATION_KEY,
                                                                 HUB_STUDYLINK_APPLICATION_COURSE_KEY, APPLICATIONID,
                                                                 COURSE_CODE, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(A.APPLICATIONID || ',' ||
           IFNULL(A.COURSE_CODE, ''))              LNK_STUDYLINK_APPLICATION_TO_COURSE_KEY,
       MD5(A.APPLICATIONID)                        HUB_STUDYLINK_APPLICATION_KEY,
       MD5(A.APPLICATIONID || ',' ||
           IFNULL(A.COURSE_CODE, ''))              HUB_STUDYLINK_APPLICATION_COURSE_KEY,
       A.APPLICATIONID,
       A.COURSE_CODE,
       'AMIS'                                      SOURCE,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM (
         SELECT DF_APPLICATIONDETAILS_APPLICATIONID APPLICATIONID,
                PREFERENCEFIELDS_CUSTOMCOURSECODE   COURSE_CODE
         FROM ODS.STUDYLINK.STUDYLINK_APPLICATION_COURSE_API_LATEST
         UNION
         SELECT APPLICATIONID, COURSE_CODE
         FROM (
                  SELECT A.APPLICATIONID,
                         DECODE(R.SEQ, 0, DEG_S1_CODE,
                                1, FND_S1_CODE,
                                2, DIP_S1_CODE,
                                3, ENG_S1_CODE,
                                4, GCGD_S1_CODE,
                                5, SAE_S1_CODE) COURSE_CODE
                  FROM ODS.STUDYLINK.STUDYLINK_APPLICATION_CSV_EXTRACT A
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
                  WHERE DECODE(R.SEQ
                            , 0
                            , DEG_S1_CODE
                            , 1
                            , FND_S1_CODE
                            , 2
                            , DIP_S1_CODE
                            , 3
                            , ENG_S1_CODE
                            , 4
                            , GCGD_S1_CODE
                            , 5
                            , SAE_S1_CODE) IS NOT NULL
              )
              ) A
         WHERE NOT EXISTS(
                 SELECT NULL
                 FROM DATA_VAULT.CORE.LNK_STUDYLINK_APPLICATION_TO_COURSE L
                 WHERE L.HUB_STUDYLINK_APPLICATION_KEY = MD5(A.APPLICATIONID)
                   AND L.HUB_STUDYLINK_APPLICATION_COURSE_KEY = MD5(A.APPLICATIONID || ',' ||
                                                                    IFNULL(A.COURSE_CODE, ''))
             );
