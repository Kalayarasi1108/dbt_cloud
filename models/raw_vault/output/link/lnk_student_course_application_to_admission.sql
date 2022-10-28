INSERT INTO DATA_VAULT.CORE.LNK_STUDENT_COURSE_APPLICATION_TO_ADMISSION (LNK_STUDENT_COURSE_APPLICATION_TO_ADMISSION_KEY,
                                                                    HUB_COURSE_APPLICATION_KEY,
                                                                    HUB_COURSE_ADMISSION_KEY,
                                                                    STU_ID,
                                                                    COURSE_SPK_CD,
                                                                    COURSE_SPK_VER_NO,
                                                                    COURSE_AVAIL_YR,
                                                                    COURSE_LOCATION_CD,
                                                                    COURSE_SPRD_CD,
                                                                    COURSE_AVAIL_KEY_NO,
                                                                    COURSE_SSP_NO,
                                                                    APPLICATION_SPK_NO,
                                                                    APPLICATION_SPK_VER_NO,
                                                                    APPLICATION_APPN_NO,
                                                                    SOURCE, LOAD_DTS, ETL_JOB_ID)


WITH APPLICATION AS (
    SELECT S.STU_ID,
           S.SPK_NO,
           S.SPK_VER_NO,
           L.APPLICATION_ID,
           L.APPLICATION_LINE_ID,
           CONCAT(L.APPLICATION_ID, '_', L.APPLICATION_LINE_ID) AS APPN_NO,
           S.SSP_NO,
           S.AVAIL_KEY_NO,
           AVAIL_DET.AVAIL_YR
    FROM ODS.AMIS.S1APP_APPLICATION AS A
             INNER JOIN ODS.AMIS.S1APP_APPLICATION_LINE AS L ON A.APPLICATION_ID = L.APPLICATION_ID
             INNER JOIN ODS.AMIS.S1APP_STUDY AS S
                        ON L.APPLICATION_ID = S.APPLICATION_ID AND S.APPLICATION_LINE_ID = L.APPLICATION_LINE_ID
             INNER JOIN ODS.AMIS.S1SPK_AVAIL_DET AVAIL_DET
                        ON AVAIL_DET.AVAIL_KEY_NO = S.AVAIL_KEY_NO
             LEFT JOIN ODS.AMIS.S1APP_OFFER AS O ON O.APPLICATION_ID = L.APPLICATION_ID
        AND L.APPLICATION_LINE_ID = O.APPLICATION_LINE_ID
),
     CURRENT_APP AS (
         SELECT MD5(
                            IFNULL(CS_SSP.STU_ID, '') || ',' ||
                            IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                            IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                            IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                            IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                            IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                            IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                            IFNULL(CS_SSP.SSP_NO, 0) || ',' ||
                            IFNULL(APPLICATION.SPK_NO, 0) || ',' ||
                            IFNULL(APPLICATION.SPK_VER_NO, 0) || ',' ||
                            IFNULL(APPLICATION.APPN_NO, '')
                    )                                LNK_STUDENT_COURSE_APPLICATION_TO_ADMISSION_KEY,
                MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                    IFNULL(APPLICATION.SPK_NO, 0) || ',' ||
                    IFNULL(APPLICATION.SPK_VER_NO, 0) || ',' ||
                    IFNULL(APPLICATION.APPN_NO, '')) HUB_COURSE_APPLICATION_KEY,
                MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                    IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                    IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                    IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                    IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                    IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(CS_SSP.SSP_NO, 0)
                    )                                HUB_COURSE_ADMISSION_KEY,
                CS_SSP.STU_ID                        STU_ID,
                CS_SPK_DET.SPK_CD                    COURSE_SPK_CD,
                CS_SPK_DET.SPK_VER_NO                COURSE_SPK_VER_NO,
                CS_SSP.AVAIL_YR                      COURSE_AVAIL_YR,
                CS_SSP.LOCATION_CD                   COURSE_LOCATION_CD,
                CS_SSP.SPRD_CD                       COURSE_SPRD_CD,
                CS_SSP.AVAIL_KEY_NO                  COURSE_AVAIL_KEY_NO,
                CS_SSP.SSP_NO                        COURSE_SSP_NO,
                APPLICATION.SPK_NO                   APPLICATION_SPK_NO,
                APPLICATION.SPK_VER_NO               APPLICATION_SPK_VER_NO,
                APPLICATION.APPN_NO                  APPLICATION_APPN_NO

         FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                  JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                       ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                           AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
                  JOIN APPLICATION
                       ON APPLICATION.STU_ID = CS_SSP.STU_ID
                              AND APPLICATION.SSP_NO = CS_SSP.SSP_NO
         WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
     )
SELECT APP.LNK_STUDENT_COURSE_APPLICATION_TO_ADMISSION_KEY,
       APP.HUB_COURSE_APPLICATION_KEY,
       APP.HUB_COURSE_ADMISSION_KEY,
       APP.STU_ID,
       APP.COURSE_SPK_CD,
       APP.COURSE_SPK_VER_NO,
       APP.COURSE_AVAIL_YR,
       APP.COURSE_LOCATION_CD,
       APP.COURSE_SPRD_CD,
       APP.COURSE_AVAIL_KEY_NO,
       APP.COURSE_SSP_NO,
       APP.APPLICATION_SPK_NO,
       APP.APPLICATION_SPK_VER_NO,
       APP.APPLICATION_APPN_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM CURRENT_APP APP

WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_STUDENT_COURSE_APPLICATION_TO_ADMISSION L
        WHERE L.LNK_STUDENT_COURSE_APPLICATION_TO_ADMISSION_KEY = APP.LNK_STUDENT_COURSE_APPLICATION_TO_ADMISSION_KEY
    );

