INSERT INTO
    DATA_VAULT.CORE.HUB_UNIT_OFFERING (
        HUB_UNIT_OFFERING_KEY,
        SPK_CD,
        SPK_VER_NO,
        AVAIL_KEY_NO,
        AVAIL_YR,
        SPRD_CD,
        LOCATION_CD,
        SOURCE,
        LOAD_DTS,
        ETL_JOB_ID
    )
SELECT
    MD5(
        IFNULL(HUO.SPK_CD, '') || ',' ||
        IFNULL(HUO.SPK_VER_NO, 0) || ',' ||
        IFNULL(HUO.AVAIL_KEY_NO, 0) || ',' ||
        IFNULL(HUO.AVAIL_YR, 0) || ',' ||
        IFNULL(HUO.SPRD_CD, '') || ',' ||
        IFNULL(HUO.LOCATION_CD, '')
    ) HUB_UNIT_OFFERING_KEY,
    HUO.SPK_CD,
    HUO.SPK_VER_NO,
    HUO.AVAIL_KEY_NO,
    HUO.AVAIL_YR,
    HUO.SPRD_CD,
    HUO.LOCATION_CD,
    'AMIS' SOURCE,
    CURRENT_TIMESTAMP :: TIMESTAMP_NTZ LOAD_DTS,
    'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM
    (
        SELECT
            SPK_CD,
            SD.SPK_VER_NO,
            AVAIL_KEY_NO,
            AVAIL_YR,
            SPRD_CD,
            LOCATION_CD
        FROM
            ODS.AMIS.S1SPK_DET SD
            JOIN ODS.AMIS.S1SPK_AVAIL_DET SAD ON SD.SPK_NO = SAD.SPK_NO
            AND SD.SPK_VER_NO = SAD.SPK_VER_NO
        WHERE
            SPK_CAT_CD = 'UN'
        UNION
        SELECT
            SPK_CD,
            SD.SPK_VER_NO,
            AVAIL_KEY_NO,
            AVAIL_YR,
            SPRD_CD,
            LOCATION_CD
        FROM
            ODS.AMIS.S1SPK_DET SD
            JOIN ODS.AMIS.S1SSP_STU_SPK SSS ON SD.SPK_NO = SSS.SPK_NO
            AND SD.SPK_VER_NO = SSS.SPK_VER_NO
        WHERE
            SPK_CAT_CD = 'UN'
            AND AVAIL_KEY_NO = 0
    ) HUO
WHERE
    NOT EXISTS(
        SELECT
            NULL
        FROM
            DATA_VAULT.CORE.HUB_UNIT_OFFERING H
        WHERE
            H.HUB_UNIT_OFFERING_KEY = MD5(
                IFNULL(HUO.SPK_CD, '') || ',' ||
                IFNULL(HUO.SPK_VER_NO, 0) || ',' ||
                IFNULL(HUO.AVAIL_KEY_NO, 0) || ',' ||
                IFNULL(HUO.AVAIL_YR, 0) || ',' ||
                IFNULL(HUO.SPRD_CD, '') || ',' ||
                IFNULL(HUO.LOCATION_CD, '')
            )
    );