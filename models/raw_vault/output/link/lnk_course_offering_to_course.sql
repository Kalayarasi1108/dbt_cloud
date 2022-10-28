INSERT INTO DATA_VAULT.CORE.LNK_COURSE_OFFERING_TO_COURSE (LNK_COURSE_OFFERING_TO_COURSE_KEY, HUB_COURSE_OFFERING_KEY,
                                                           HUB_COURSE_KEY, COURSE_SPK_CD, COURSE_SPK_VER_NO,
                                                           COURSE_AVAIL_YR, COURSE_LOCATION_CD, COURSE_SPRD_CD,
                                                           COURSE_AVAIL_KEY_NO, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(IFNULL(HUO.SPK_CD, '') || ',' ||
           IFNULL(HUO.SPK_VER_NO, 0) || ',' ||
           IFNULL(HUO.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(HUO.AVAIL_YR, 0) || ',' ||
           IFNULL(HUO.SPRD_CD, '') || ',' ||
           IFNULL(HUO.LOCATION_CD, '')
           )                                     LNK_COURSE_ADMISSION_TO_OFFERING_KEY,
       MD5(
                   IFNULL(HUO.SPK_CD, '') || ',' ||
                   IFNULL(HUO.SPK_VER_NO, 0) || ',' ||
                   IFNULL(HUO.AVAIL_KEY_NO, 0) || ',' ||
                   IFNULL(HUO.AVAIL_YR, 0) || ',' ||
                   IFNULL(HUO.SPRD_CD, '') || ',' ||
                   IFNULL(HUO.LOCATION_CD, '')
           )                                     HUB_COURSE_OFFERING_KEY,
       MD5(IFNULL(HUO.SPK_CD, '') || ',' ||
           IFNULL(HUO.SPK_VER_NO, 0)
           )                                     HUB_COURSE_ADMISSION_KEY,
       SPK_CD                                    COURSE_SPK_CD,
       SPK_VER_NO                                COURSE_SPK_VER_NO,
       AVAIL_YR                                  COURSE_AVAIL_YR,
       LOCATION_CD                               COURSE_LOCATION_CD,
       SPRD_CD                                   COURSE_SPRD_CD,
       AVAIL_KEY_NO                              COURSE_AVAIL_KEY_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM (SELECT SD.SPK_CD,
             SD.SPK_VER_NO,
             SAD.AVAIL_KEY_NO,
             SAD.AVAIL_YR,
             SAD.SPRD_CD,
             SAD.LOCATION_CD
      FROM ODS.AMIS.S1SPK_DET SD
               JOIN ODS.AMIS.S1SPK_AVAIL_DET SAD ON SD.SPK_NO = SAD.SPK_NO
          AND SD.SPK_VER_NO = SAD.SPK_VER_NO
      WHERE SPK_CAT_CD = 'CS') HUO
WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_COURSE_OFFERING_TO_COURSE L
        WHERE L.HUB_COURSE_OFFERING_KEY = MD5(IFNULL(HUO.SPK_CD, '') || ',' ||
                                              IFNULL(HUO.SPK_VER_NO, 0) || ',' ||
                                              IFNULL(HUO.AVAIL_KEY_NO, 0) || ',' ||
                                              IFNULL(HUO.AVAIL_YR, 0) || ',' ||
                                              IFNULL(HUO.SPRD_CD, '') || ',' ||
                                              IFNULL(HUO.LOCATION_CD, '')
            )
          AND L.HUB_COURSE_KEY = MD5(
                    IFNULL(HUO.SPK_CD, '') || ',' ||
                    IFNULL(HUO.SPK_VER_NO, 0)
            )
    )
;
