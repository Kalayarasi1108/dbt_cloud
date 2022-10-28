SELECT
       MD5(
            IFNULL(STRUC_SPK_DET.SPK_CD, '') || ',' ||
            IFNULL(STRUC_SPK_DET.SPK_VER_NO, 0)
           )                                       HUB_STRUCTURE_KEY,
       STRUC_SPK_DET.SPK_CD STRUCTURE_SPK_CD,
       STRUC_SPK_DET.SPK_VER_NO STRUCTURE_SPK_VER_NO,
       'AMIS'                                      source,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM {{source('AMIS','S1SSP_DET')}} STRUC_SPK_DET
WHERE SPK_CAT_CD IN (
                     'SS',
                     'MJ',
                     'SP',
                     'MN',
                     'ST'
    )
AND NOT EXISTS(
        SELECT NULL
        FROM {{source('CORE','HUB_STRUCTURE')}} H
        WHERE H.HUB_STRUCTURE_KEY = MD5(
                                    IFNULL(STRUC_SPK_DET.SPK_CD, '') || ',' ||
                                    IFNULL(STRUC_SPK_DET.SPK_VER_NO, 0)
           )
    );