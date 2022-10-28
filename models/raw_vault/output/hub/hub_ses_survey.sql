SELECT MD5(IFNULL(SES_MQ.SESID, ''))               HUB_SES_SURVEY_KEY,
       SES_MQ.SESID,
       'QILT'                                      SOURCE,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM {{source('QILT','SES_MQ_PIVOT')}} SES_MQ
WHERE NOT EXISTS(
        SELECT NULL
        FROM {{source('CORE','HUB_SES_SURVEY')}} H
        WHERE H.HUB_SES_SURVEY_KEY = MD5(IFNULL(SES_MQ.SESID, ''))
    );



