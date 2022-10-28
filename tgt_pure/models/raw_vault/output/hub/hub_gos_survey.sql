
SELECT MD5(IFNULL(GOS_MQ.GOSID, ''))               HUB_GOS_SURVEY_KEY,
       GOS_MQ.GOSID,
       'QILT'                                      SOURCE,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.QILT.GOS_MQ_PIVOT GOS_MQ
WHERE NOT EXISTS(
        SELECT NULL
        FROM {{source('CORE','HUB_GOS_SURVEY')}} H
        WHERE H.HUB_GOS_SURVEY_KEY = MD5(IFNULL(GOS_MQ.GOSID, ''))
    );