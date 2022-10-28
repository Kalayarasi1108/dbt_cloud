INSERT INTO DATA_VAULT.CORE.HUB_UNIT (HUB_UNIT_KEY, SPK_CD, SPK_VER_NO, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(
                   IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                   IFNULL(UN_SPK_DET.SPK_VER_NO, 0)
           )                                       HUB_UNIT_KEY,
       UN_SPK_DET.SPK_CD,
       UN_SPK_DET.SPK_VER_NO,
       'AMIS'                                      SOURCE,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SPK_DET UN_SPK_DET
WHERE SPK_CAT_CD = 'UN'
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.HUB_UNIT H
        WHERE H.HUB_UNIT_KEY = MD5(
                    IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_VER_NO, 0)
            )
    );