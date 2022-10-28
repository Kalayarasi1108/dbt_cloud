INSERT INTO DATA_VAULT.CORE.LNK_UNIT_ENROLMENT_TO_STATUS (LNK_UNIT_ENROLMENT_TO_STATUS_KEY,
                                                          HUB_UNIT_ENROLMENT_KEY,
                                                          HUB_UNIT_ENROLMENT_STATUS_KEY,
                                                          STU_ID,
                                                          SPK_CD,
                                                          SPK_VER_NO,
                                                          AVAIL_YR,
                                                          LOCATION_CD,
                                                          SPRD_CD,
                                                          AVAIL_KEY_NO,
                                                          SSP_NO,
                                                          PARENT_SSP_NO,
                                                          SSP_STTS_NO,
                                                          SOURCE,
                                                          LOAD_DTS,
                                                          ETL_JOB_ID)
SELECT MD5(IFNULL(DET.STU_ID, '') || ',' ||
           IFNULL(DET.SPK_CD, '') || ',' ||
           IFNULL(DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(DET.AVAIL_YR, 0) || ',' ||
           IFNULL(DET.LOCATION_CD, '') || ',' ||
           IFNULL(DET.SPRD_CD, '') || ',' ||
           IFNULL(DET.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(DET.SSP_NO, 0) || ',' ||
           IFNULL(DET.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(DET.SSP_STTS_NO, 0)
           )                                       LNK_STUDENT_UNIT_ENROLMENT_KEY,
       MD5(IFNULL(DET.STU_ID, '') || ',' ||
           IFNULL(DET.SPK_CD, '') || ',' ||
           IFNULL(DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(DET.AVAIL_YR, 0) || ',' ||
           IFNULL(DET.LOCATION_CD, '') || ',' ||
           IFNULL(DET.SPRD_CD, '') || ',' ||
           IFNULL(DET.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(DET.SSP_NO, 0) || ',' ||
           IFNULL(DET.PARENT_SSP_NO, 0)
           )                                       HUB_UNIT_ENROLMENT_KEY,
       MD5(IFNULL(DET.STU_ID, '') || ',' ||
           IFNULL(DET.SSP_NO, 0) || ',' ||
           IFNULL(DET.SSP_STTS_NO, 0)
           )                                       HUB_UNIT_ENROLMENT_STATUS_KEY,
       DET.STU_ID,
       DET.SPK_CD,
       DET.SPK_VER_NO,
       DET.AVAIL_YR,
       DET.LOCATION_CD,
       DET.SPRD_CD,
       DET.AVAIL_KEY_NO,
       DET.SSP_NO,
       DET.PARENT_SSP_NO,
       DET.SSP_STTS_NO,
       'AMIS'                                      SOURCE,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM (
         SELECT UN_SSP_DETAILS.STU_ID,
                UN_SSP_DETAILS.SPK_CD,
                UN_SSP_DETAILS.SPK_VER_NO,
                UN_SSP_DETAILS.AVAIL_YR,
                UN_SSP_DETAILS.LOCATION_CD,
                UN_SSP_DETAILS.SPRD_CD,
                IFNULL(UN_SSP_DETAILS.AVAIL_KEY_NO, 0) AVAIL_KEY_NO,
                UN_SSP_DETAILS.SSP_NO,
                UN_SSP_DETAILS.PARENT_SSP_NO,
                UN_STTS_DETAILS.SSP_STTS_NO
         FROM (
                  SELECT UN_SSP.STU_ID,
                         UN_SPK_DET.SPK_CD,
                         UN_SPK_DET.SPK_VER_NO,
                         UN_SSP.AVAIL_YR,
                         UN_SSP.LOCATION_CD,
                         UN_SSP.SPRD_CD,
                         IFNULL(UN_SSP.AVAIL_KEY_NO, 0) AVAIL_KEY_NO,
                         UN_SSP.SSP_NO,
                         UN_SSP.PARENT_SSP_NO
                  FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
                           JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                      AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                      AND UN_SPK_DET.SPK_CAT_CD = 'UN'
                  WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
              ) UN_SSP_DETAILS
                  INNER JOIN
              (
                  SELECT UN_SSP_STTS.STU_ID,
                         UN_SSP_STTS.SSP_NO,
                         UN_SSP_STTS.SSP_STTS_NO
                  FROM ODS.AMIS.S1SSP_STTS_HIST UN_SSP_STTS
                           JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET ON UN_SPK_DET.SPK_NO = UN_SSP_STTS.SPK_NO
                      AND UN_SPK_DET.SPK_VER_NO = UN_SSP_STTS.SPK_VER_NO
                      AND UN_SPK_DET.SPK_CAT_CD = 'UN'
              ) UN_STTS_DETAILS
              ON UN_SSP_DETAILS.SSP_NO = UN_STTS_DETAILS.SSP_NO
                  AND UN_SSP_DETAILS.STU_ID = UN_STTS_DETAILS.STU_ID
     ) DET
WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_UNIT_ENROLMENT_TO_STATUS L
        WHERE L.HUB_UNIT_ENROLMENT_KEY = MD5(IFNULL(DET.STU_ID, '') || ',' ||
                                             IFNULL(DET.SPK_CD, '') || ',' ||
                                             IFNULL(DET.SPK_VER_NO, 0) || ',' ||
                                             IFNULL(DET.AVAIL_YR, 0) || ',' ||
                                             IFNULL(DET.LOCATION_CD, '') || ',' ||
                                             IFNULL(DET.SPRD_CD, '') || ',' ||
                                             IFNULL(DET.AVAIL_KEY_NO, 0) || ',' ||
                                             IFNULL(DET.SSP_NO, 0) || ',' ||
                                             IFNULL(DET.PARENT_SSP_NO, 0)
            )

          AND L.HUB_UNIT_ENROLMENT_STATUS_KEY = MD5(IFNULL(DET.STU_ID, '') || ',' ||
                                                    IFNULL(DET.SSP_NO, 0) || ',' ||
                                                    IFNULL(DET.SSP_STTS_NO, 0)
            )
    );