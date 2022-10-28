SELECT MD5(A.APPLICATIONID || ',' ||
           IFNULL(A.COURSE_CODE, ''))            HUB_STUDYLINK_APPLICATION_COURSE_KEY,
       A.APPLICATIONID                           APPLICATIONID,
       A.COURSE_CODE                             COURSE_CODE,
       'STUDYLINK'                               SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM (
         SELECT DF_APPLICATIONDETAILS_APPLICATIONID APPLICATIONID,
                PREFERENCEFIELDS_CUSTOMCOURSECODE   COURSE_CODE
         FROM {{source('STUDYLINK','STUDYLINK_APPLICATION_COURSE_API_LATEST')}}
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
                  FROM {{source('STUDYLINK','STUDYLINK_APPLICATION_CSV_EXTRACT')}} A
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
     ) E ) A
WHERE NOT EXISTS (
    SELECT 1
    FROM {{source('CORE','HUB_STUDYLINK_APPLICATION_COURSE')}} S
    WHERE S.HUB_STUDYLINK_APPLICATION_COURSE_KEY = MD5(A.APPLICATIONID || ',' ||
    A.COURSE_CODE)
    );
