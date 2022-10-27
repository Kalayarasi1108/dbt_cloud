INSERT INTO DATA_VAULT.CORE.HUB_COURSE (HUB_COURSE_KEY, SPK_CD, SPK_VER_NO, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT
       MD5(
            IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
            IFNULL(CS_SPK_DET.SPK_VER_NO, 0)
           )                                       HUB_UNIT_KEY,
       CS_SPK_DET.SPK_CD,
       CS_SPK_DET.SPK_VER_NO,
       'AMIS'                                      SOURCE,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SPK_DET CS_SPK_DET
WHERE SPK_CAT_CD = 'CS'
AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.HUB_COURSE H
        WHERE H.HUB_COURSE_KEY = MD5(
                                    IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                                    IFNULL(CS_SPK_DET.SPK_VER_NO, 0)
           )
    );