INSERT INTO DATA_VAULT.CORE.LNK_COURSE_APPLICATION_TO_OFFERING(LNK_COURSE_APPLICATION_TO_OFFERING_KEY,
                                                               HUB_COURSE_OFFERING_KEY, HUB_COURSE_APPLICATION_KEY,
                                                               APPLICATION_STU_ID, APPLICATION_SPK_NO,
                                                               APPLICATION_SPK_VER_NO, APPLICATION_APPN_NO,
                                                               COURSE_SPK_CD, COURSE_SPK_VER_NO, COURSE_AVAIL_YR,
                                                               COURSE_LOCATION_CD, COURSE_SPRD_CD, COURSE_AVAIL_KEY_NO,
                                                               SOURCE, LOAD_DTS, ETL_JOB_ID)

WITH S1APP_APPLICATION AS (
    SELECT MD5(IFNULL(APP_STUDY.STU_ID, '') || ',' ||
               IFNULL(APP_STUDY.SPK_NO, 0) || ',' ||
               IFNULL(APP_STUDY.SPK_VER_NO, 0) || ',' ||
               (CONCAT(APP_STUDY.APPLICATION_ID, '_', APP_STUDY.APPLICATION_LINE_ID)))
                                                                                   HUB_COURSE_APPLICATION_KEY,
           APP_STUDY.STU_ID,
           APP_STUDY.SPK_NO,
           APP_STUDY.SPK_VER_NO,
           CONCAT(APP_STUDY.APPLICATION_ID, '_', APP_STUDY.APPLICATION_LINE_ID) AS APPN_NO,
           APP_STUDY.AVAIL_KEY_NO,
           SSP_NO

    FROM ODS.AMIS.S1APP_APPLICATION S1APP
             INNER JOIN ODS.AMIS.S1APP_APPLICATION_LINE APP_LINE
                        ON S1APP.APPLICATION_ID = APP_LINE.APPLICATION_ID
             INNER JOIN ODS.AMIS.S1APP_STUDY APP_STUDY
                        ON APP_LINE.APPLICATION_ID = APP_STUDY.APPLICATION_ID
                            AND APP_STUDY.APPLICATION_LINE_ID = APP_LINE.APPLICATION_LINE_ID
             INNER JOIN ODS.AMIS.S1SPK_AVAIL_DET AVAIL_DET
                        ON AVAIL_DET.AVAIL_KEY_NO = APP_STUDY.AVAIL_KEY_NO
)
   , S1STU_APPLICATION AS
    (
        SELECT MD5(IFNULL(S1STU_APP.STU_ID, '') || ',' ||
                   IFNULL(S1STU_APP.SPK_NO, 0) || ',' ||
                   IFNULL(S1STU_APP.SPK_VER_NO, 0) || ',' ||
                   IFNULL(S1STU_APP.APPN_NO, '0'))
                                          HUB_COURSE_APPLICATION_KEY,
               S1STU_APP.STU_ID,
               S1STU_APP.SPK_NO,
               S1STU_APP.SPK_VER_NO,
               S1STU_APP.APPN_NO::VARCHAR APPN_NO,
               CS_SSP.AVAIL_KEY_NO,
               CS_SSP.SSP_NO

        FROM ODS.AMIS.S1STU_APPLICATION S1STU_APP
                 JOIN ODS.AMIS.S1SSP_STU_SPK CS_SSP
                      ON S1STU_APP.SSP_NO = CS_SSP.SSP_NO
    )

   , HUB_APPLICATION_UNION AS (
    SELECT *
    FROM S1APP_APPLICATION
    UNION
    SELECT *
    FROM S1STU_APPLICATION
)

   , HUB_COUSE_OFFERING AS (
    SELECT MD5(
                       IFNULL(SPK_DET.SPK_CD, '') || ',' ||
                       IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
                       IFNULL(SPK_AVAIL_DET.AVAIL_KEY_NO, 0) || ',' ||
                       IFNULL(SPK_AVAIL_DET.AVAIL_YR, 0) || ',' ||
                       IFNULL(SPK_AVAIL_DET.SPRD_CD, '') || ',' ||
                       IFNULL(SPK_AVAIL_DET.LOCATION_CD, '')
               )              HUB_COURSE_OFFERING_KEY,
           SPK_DET.SPK_CD,
           SPK_DET.SPK_VER_NO SPK_VER_NO,
           SPK_AVAIL_DET.AVAIL_KEY_NO,
           SPK_AVAIL_DET.AVAIL_YR,
           SPK_AVAIL_DET.SPRD_CD,
           SPK_AVAIL_DET.LOCATION_CD

    FROM ODS.AMIS.S1SPK_DET SPK_DET
             JOIN ODS.AMIS.S1SPK_AVAIL_DET SPK_AVAIL_DET ON SPK_DET.SPK_NO = SPK_AVAIL_DET.SPK_NO
        AND SPK_DET.SPK_VER_NO = SPK_AVAIL_DET.SPK_VER_NO AND SPK_CAT_CD = 'CS'
)
   , LNK_COURSE_OFFERING_APPLICATION AS (
    SELECT MD5(
                       IFNULL(HUB_APP.STU_ID, '') || ',' ||
                       IFNULL(HUB_APP.SPK_NO, 0) || ',' ||
                       IFNULL(HUB_APP.SPK_VER_NO, 0) || ',' ||
                       IFNULL(HUB_APP.APPN_NO, '0') || ',' ||
                       IFNULL(HUB_OFFERING.SPK_CD, '') || ',' ||
                       IFNULL(HUB_OFFERING.SPK_VER_NO, 0) || ',' ||
                       IFNULL(HUB_OFFERING.AVAIL_KEY_NO, 0) || ',' ||
                       IFNULL(HUB_OFFERING.AVAIL_YR, 0) || ',' ||
                       IFNULL(HUB_OFFERING.SPRD_CD, '') || ',' ||
                       IFNULL(HUB_OFFERING.LOCATION_CD, '')
               )                     LNK_COURSE_APPLICATION_TO_OFFERING_KEY,
           HUB_APP.HUB_COURSE_APPLICATION_KEY,
           HUB_OFFERING.HUB_COURSE_OFFERING_KEY,
           HUB_APP.STU_ID            APPLICATION_STU_ID,
           HUB_APP.SPK_NO            APPLICATION_SPK_NO,
           HUB_APP.SPK_VER_NO        APPLICATION_SPK_VER_NO,
           HUB_APP.APPN_NO           APPLICATION_APPN_NO,
           HUB_OFFERING.SPK_CD       COURSE_SPK_CD,
           HUB_OFFERING.SPK_VER_NO   COURSE_SPK_VER_NO,
           HUB_OFFERING.AVAIL_KEY_NO COURSE_AVAIL_KEY_NO,
           HUB_OFFERING.AVAIL_YR     COURSE_AVAIL_YR,
           HUB_OFFERING.SPRD_CD      COURSE_SPRD_CD,
           HUB_OFFERING.LOCATION_CD  COURSE_LOCATION_CD


    FROM HUB_APPLICATION_UNION HUB_APP
             JOIN HUB_COUSE_OFFERING HUB_OFFERING ON
        HUB_APP.AVAIL_KEY_NO = HUB_OFFERING.AVAIL_KEY_NO
)
SELECT LNK_COURSE_APPLICATION_TO_OFFERING_KEY,
       HUB_COURSE_OFFERING_KEY,
       HUB_COURSE_APPLICATION_KEY,
       APPLICATION_STU_ID,
       APPLICATION_SPK_NO,
       APPLICATION_SPK_VER_NO,
       APPLICATION_APPN_NO,
       COURSE_SPK_CD,
       COURSE_SPK_VER_NO,
       COURSE_AVAIL_YR,
       COURSE_LOCATION_CD,
       COURSE_SPRD_CD,
       COURSE_AVAIL_KEY_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID

FROM LNK_COURSE_OFFERING_APPLICATION LNK

WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_COURSE_APPLICATION_TO_OFFERING L
        WHERE L.HUB_COURSE_APPLICATION_KEY = LNK.HUB_COURSE_APPLICATION_KEY
          AND L.HUB_COURSE_OFFERING_KEY = LNK.HUB_COURSE_OFFERING_KEY
    );