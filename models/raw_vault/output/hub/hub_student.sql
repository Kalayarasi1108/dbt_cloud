SELECT A.HUB_STUDENT_KEY,
       A.STU_ID,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM (SELECT MD5(STU_ID)                                        HUB_STUDENT_KEY,
             STU_ID                                             STU_ID,
             ROW_NUMBER() OVER (PARTITION BY STU_ID ORDER BY 1) RN
      FROM {{source('AMIS','S1STU_DET')}}) A
WHERE A.RN = 1
  AND NOT EXISTS(
        SELECT 1
        FROM {{source('CORE','HUB_STUDENT')}} S
        WHERE S.HUB_STUDENT_KEY = MD5(A.STU_ID)
    );