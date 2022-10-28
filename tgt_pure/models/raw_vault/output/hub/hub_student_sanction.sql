SELECT MD5(
                   IFNULL(STU_SANCTION.STU_ID, '') || ',' ||
                   IFNULL(STU_SANCTION.SEQ_NO, 0)
           )                                       HUB_STUDENT_SANCTION_KEY,
       STU_SANCTION.STU_ID,
       STU_SANCTION.SEQ_NO,
       'AMIS'                                      SOURCE,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM {{source('AMIS','S1STU_SANCTION')}} STU_SANCTION
WHERE NOT EXISTS(
        SELECT NULL
        FROM {{source('CORE','HUB_STUDENT_SANCTION')}} H
        WHERE H.HUB_STUDENT_SANCTION_KEY = MD5(
                    IFNULL(STU_SANCTION.STU_ID, '') || ',' ||
                    IFNULL(STU_SANCTION.SEQ_NO, 0)
            )
    );