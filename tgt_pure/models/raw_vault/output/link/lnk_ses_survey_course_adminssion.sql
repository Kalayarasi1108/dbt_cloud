INSERT INTO DATA_VAULT.CORE.LNK_SES_SURVEY_COURSE_ADMISSION(LNK_SES_SURVEY_COURSE_ADMISSION_KEY, HUB_SES_SURVEY_KEY,
                                                            HUB_COURSE_ADMISSION_KEY, SESID, COURSE_STU_ID,
                                                            COURSE_SPK_CD,
                                                            COURSE_SPK_VER_NO, COURSE_AVAIL_YR, COURSE_LOCATION_CD,
                                                            COURSE_SPRD_CD, COURSE_AVAIL_KEY_NO, COURSE_SSP_NO,
                                                            REPAR_COURSE,
                                                            REPAR_SSP_NO, REPAR_COURSE_CODE, SOURCE, LOAD_DTS,
                                                            ETL_JOB_ID)
WITH SPK_CAT_CD_CS AS (
    SELECT CS_SPK_DET.SPK_NO,
           CS_SPK_DET.SPK_VER_NO,
           CS_SPK_FOE.FOE_CD,
           CS_SPK_FOE.PRIM_SECONDARY_CD,
           FOE_DET.FOE_TRLN_CD,
           FOE_DET.FOE_DESC
    FROM ODS.AMIS.S1SPK_DET CS_SPK_DET
             JOIN ODS.AMIS.S1SPK_FOE CS_SPK_FOE
                  ON CS_SPK_FOE.SPK_NO = CS_SPK_DET.SPK_NO AND
                     CS_SPK_FOE.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
             JOIN ODS.AMIS.S1FOE_DET FOE_DET
                  ON FOE_DET.FOE_CD = CS_SPK_FOE.FOE_CD
    WHERE CS_SPK_DET.SPK_CAT_CD = 'CS'
)
   , FOE AS (
    SELECT A.SPK_NO,
           A.SPK_VER_NO,
           MAX(IFF(A.PRIM_SECONDARY_CD = 'P', A.FOE_TRLN_CD, NULL)) PRIMARY_FIELD_OF_EDUCATION_CODE,
           MAX(IFF(A.PRIM_SECONDARY_CD = 'P', A.FOE_DESC, NULL))    PRIMARY_FIELD_OF_EDUCATION_DESC,
           MAX(IFF(A.PRIM_SECONDARY_CD = 'S', A.FOE_TRLN_CD, NULL)) SECONDARY_FIELD_OF_EDUCATION_CODE,
           MAX(IFF(A.PRIM_SECONDARY_CD = 'S', A.FOE_DESC, NULL))    SECONDARY_FIELD_OF_EDUCATION_DESC
    FROM SPK_CAT_CD_CS A
    GROUP BY A.SPK_NO, A.SPK_VER_NO
)

   , ALL_E307 AS (
    SELECT CS_SSP.STU_ID
         , SPK_DET.SPK_VER_NO
         , CS_SSP.AVAIL_YR
         , CS_SSP.LOCATION_CD
         , CS_SSP.SPRD_CD
         , CS_SSP.AVAIL_KEY_NO
         , CS_SSP.SSP_NO
         , CS_SSP.PARENT_SSP_NO
         , CS_SSP.PARENT_SSP_ATT_NO
         , CS_SSP.SSP_STG_CD
         , IFF(CS_SSP.REPAR_SSP_NO > 0, 'Y', 'N')                              REPARENTED_COURSE
         , CS_SSP.REPAR_SSP_NO
         , SPK_DET.SPK_CD                                                      COURSE_CODE
         , SPK_DET.EFFCT_DT                                                    EFFECTIVE_DATE
         , IFF((SPK_ALT_CRS.ALT_CRS_PREFIX is null and length(COURSE_CODE) <= 10 and YEAR(EFFECTIVE_DATE) > 2019),
               COURSE_CODE,
               SPK_ALT_CRS.ALT_CRS_PREFIX || COALESCE(FOE_SIX.CODE, '000000')) "E307"

    FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
             JOIN ODS.AMIS.S1SPK_DET SPK_DET
                  ON CS_SSP.SPK_NO = SPK_DET.SPK_NO and CS_SSP.SPK_VER_NO = SPK_DET.SPK_VER_NO
                      AND SPK_DET.SPK_CAT_CD = 'CS'
             LEFT OUTER JOIN FOE
                             ON FOE.SPK_NO = CS_SSP.SPK_NO AND FOE.SPK_VER_NO = CS_SSP.SPK_VER_NO
             LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_SIX
                             ON FOE_SIX.CODE = LPAD(COALESCE(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, '0'), 6, '0')
             LEFT JOIN ODS.AMIS.S1SPK_ALT_CRS SPK_ALT_CRS
                       ON SPK_DET.SPK_NO = SPK_ALT_CRS.SPK_NO
    WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
)
   , EXIST_E307 AS (
    SELECT STU_ID, SES.E307
    FROM ALL_E307
             JOIN ODS.QILT.SES_MQ_PIVOT SES
                  ON SES.E313 = ALL_E307.STU_ID AND SES.E307 = ALL_E307.E307
    GROUP BY STU_ID, SES.E307
)

   , REPARENT_E307 AS (
    SELECT C_REP.STU_ID                                                 STU_ID,
           C_REP.SPK_VER_NO                                             SPK_VER_NO,
           C_REP.PARENT_SSP_ATT_NO                                      PARENT_SSP_ATT_NO,
           C_REP.COURSE_CODE                                            COURSE_CODE,
           C_REP.AVAIL_YR                                               AVAIL_YR,
           C_REP.LOCATION_CD                                            LOCATION_CD,
           C_REP.SPRD_CD                                                SPRD_CD,
           C_REP.AVAIL_KEY_NO                                           AVAIL_KEY_NO,
           C_REP.SSP_NO                                                 SSP_NO,
           IFF((SELECT COUNT(*) AS COUNT
                FROM ODS.QILT.SES_MQ_PIVOT
                WHERE E313 = C_REP.STU_ID
                  AND E307 = C_E307.E307) > 0, C_E307.E307, C_REP.E307) "E307",
           EXIST_E307.E307                                              "SES_E307",

           C_REP.PARENT_SSP_NO                                          PARENT_SSP_NO,
           C_REP.REPARENTED_COURSE                                      REPARENTED_COURSE,
           C_REP.REPAR_SSP_NO                                           REPAR_SSP_NO,
           C_E307.COURSE_CODE                                           "REPAR_COURSE_CODE"
    FROM ALL_E307 C_E307
             JOIN ALL_E307 C_REP
                  ON C_REP.STU_ID = C_E307.STU_ID
                      AND C_REP.REPAR_SSP_NO = C_E307.PARENT_SSP_NO
             LEFT JOIN EXIST_E307 ON EXIST_E307.STU_ID = C_REP.STU_ID
    WHERE C_REP.PARENT_SSP_NO not in (SELECT ALL_E307.REPAR_SSP_NO FROM ALL_E307 WHERE ALL_E307.STU_ID = C_E307.STU_ID)
)

   , UNION_E307 AS (
    SELECT *
    FROM REPARENT_E307
    UNION ALL
    SELECT C_E307.STU_ID,
           C_E307.SPK_VER_NO,
           C_E307.PARENT_SSP_ATT_NO,
           C_E307.COURSE_CODE,
           C_E307.AVAIL_YR,
           C_E307.LOCATION_CD,
           C_E307.SPRD_CD,
           C_E307.AVAIL_KEY_NO,
           C_E307.SSP_NO,
           C_E307.E307 "E307",
           NULL AS     SES_E307,
           C_E307.PARENT_SSP_NO,
           'N'         REPARENTED_COURSE,
           NULL        REPAR_SSP_NO,
           NULL        REPAR_COURSE_CODE

    FROM ALL_E307 C_E307
    WHERE PARENT_SSP_NO not in (SELECT ALL_E307.REPAR_SSP_NO FROM ALL_E307 WHERE ALL_E307.STU_ID = C_E307.STU_ID)
      AND PARENT_SSP_NO not in (SELECT PARENT_SSP_NO FROM REPARENT_E307 R_E307 WHERE R_E307.STU_ID = C_E307.STU_ID)
)
   , COURSE_E307 AS (
    SELECT STU_ID,
           SPK_VER_NO,
           PARENT_SSP_ATT_NO,
           PARENT_SSP_NO,
           COURSE_CODE,
           AVAIL_YR,
           LOCATION_CD,
           SPRD_CD,
           AVAIL_KEY_NO,
           SSP_NO,
           E307,
           REPARENTED_COURSE,
           REPAR_SSP_NO,
           REPAR_COURSE_CODE,
           ROW_NUMBER()
                   OVER (PARTITION BY STU_ID,E307 ORDER BY SPK_VER_NO DESC ,PARENT_SSP_ATT_NO DESC ,AVAIL_YR DESC ,SPRD_CD DESC,AVAIL_KEY_NO DESC ) RN
    FROM (
             SELECT STU_ID,
                    SPK_VER_NO,
                    PARENT_SSP_ATT_NO,
                    PARENT_SSP_NO,
                    COURSE_CODE,
                    AVAIL_YR,
                    LOCATION_CD,
                    SPRD_CD,
                    AVAIL_KEY_NO,
                    SSP_NO,
                    IFF(SES_E307 is NULL, E307, SES_E307) "E307",
                    REPARENTED_COURSE,
                    REPAR_SSP_NO,
                    REPAR_COURSE_CODE
             FROM UNION_E307)
)

SELECT MD5(
            IFNULL(SES_MQ.SESID, '') || ',' ||
            IFNULL(COURSE_E307.STU_ID, '') || ',' ||
            IFNULL(COURSE_E307.E307, '') || ',' ||
            IFNULL(COURSE_E307.COURSE_CODE, '') || ',' ||
            IFNULL(COURSE_E307.SPK_VER_NO, 0) || ',' ||
            IFNULL(COURSE_E307.PARENT_SSP_ATT_NO, 0) || ',' ||
            IFNULL(COURSE_E307.AVAIL_YR, 0) || ',' ||
            IFNULL(COURSE_E307.LOCATION_CD, '') || ',' ||
            IFNULL(COURSE_E307.SPRD_CD, '') || ',' ||
            IFNULL(COURSE_E307.AVAIL_KEY_NO, 0) || ',' ||
            IFNULL(COURSE_E307.SSP_NO, 0)
    )                                            LNK_SES_SURVEY_COURSE_ADMISSION_KEY
     , MD5(IFNULL(SES_MQ.SESID, ''))             HUB_SES_SURVEY_KEY
     , MD5(
            IFNULL(COURSE_E307.STU_ID, '') || ',' ||
            IFNULL(COURSE_E307.COURSE_CODE, '') || ',' ||
            IFNULL(COURSE_E307.SPK_VER_NO, 0) || ',' ||
            IFNULL(COURSE_E307.AVAIL_YR, 0) || ',' ||
            IFNULL(COURSE_E307.LOCATION_CD, '') || ',' ||
            IFNULL(COURSE_E307.SPRD_CD, '') || ',' ||
            IFNULL(COURSE_E307.AVAIL_KEY_NO, 0) || ',' ||
            IFNULL(COURSE_E307.SSP_NO, 0)
    )                                            HUB_COURSE_ADMISSION_KEY

     , SES_MQ.SESID                              SESID
     , COURSE_E307.STU_ID                        COURSE_STU_ID
     , COURSE_E307.COURSE_CODE                   COURSE_SPK_CD
     , COURSE_E307.SPK_VER_NO                    COURSE_SPK_VER_NO
     , COURSE_E307.AVAIL_YR                      COURSE_AVAIL_YR
     , COURSE_E307.LOCATION_CD                   COURSE_LOCATION_CD
     , COURSE_E307.SPRD_CD                       COURSE_SPRD_CD
     , COURSE_E307.AVAIL_KEY_NO                  COURSE_AVAIL_KEY_NO
     , COURSE_E307.SSP_NO                        COURSE_SSP_NO
     , COURSE_E307.REPARENTED_COURSE             REPAR_COURSE
     , COURSE_E307.REPAR_SSP_NO                  REPAR_SSP_NO
     , COURSE_E307.REPAR_COURSE_CODE             REPAR_COURSE_CODE
     , 'QILT'                                    SOURCE
     , CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS
     , 'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.QILT.SES_MQ_PIVOT SES_MQ
         JOIN COURSE_E307 ON COURSE_E307.E307 = SES_MQ.E307
    AND COURSE_E307.STU_ID = SES_MQ.E313
WHERE COURSE_E307.RN = 1
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_SES_SURVEY_COURSE_ADMISSION L
        WHERE L.LNK_SES_SURVEY_COURSE_ADMISSION_KEY = MD5(
                    IFNULL(SES_MQ.SESID, '') || ',' ||
                    IFNULL(COURSE_E307.STU_ID, '') || ',' ||
                    IFNULL(COURSE_E307.E307, '') || ',' ||
                    IFNULL(COURSE_E307.COURSE_CODE, '') || ',' ||
                    IFNULL(COURSE_E307.SPK_VER_NO, 0) || ',' ||
                    IFNULL(COURSE_E307.PARENT_SSP_ATT_NO, 0) || ',' ||
                    IFNULL(COURSE_E307.AVAIL_YR, 0) || ',' ||
                    IFNULL(COURSE_E307.LOCATION_CD, '') || ',' ||
                    IFNULL(COURSE_E307.SPRD_CD, '') || ',' ||
                    IFNULL(COURSE_E307.AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(COURSE_E307.SSP_NO, 0)
            )
    );