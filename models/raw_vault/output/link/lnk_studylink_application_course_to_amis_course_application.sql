INSERT INTO DATA_VAULT.CORE.LNK_STUDYLINK_APPLICATION_COURSE_TO_AMIS_COURSE_APPLICATION (LNK_STUDYLINK_APPLICATION_COURSE_TO_AMIS_COURSE_APPLICATION_KEY,
                                                                                                        HUB_STUDYLINK_APPLICATION_COURSE_KEY,
                                                                                                        HUB_COURSE_APPLICATION_KEY,
                                                                                                        APPLICATION_ID,
                                                                                                        COURSE_CODE,
                                                                                                        STU_ID,
                                                                                                        SPK_NO,
                                                                                                        SPK_VER_NO,
                                                                                                        APPN_NO,
                                                                                                        SOURCE,
                                                                                                        LOAD_DTS,
                                                                                                        ETL_JOB_ID)


SELECT LNK_STUDYLINK_APPLICATION_COURSE_TO_AMIS_COURSE_APPLICATION_KEY,
       HUB_STUDYLINK_APPLICATION_COURSE_KEY,
       HUB_COURSE_APPLICATION_KEY,
       APPLICATION_ID,
       COURSE_CODE,
       STU_ID,
       SPK_NO,
       SPK_VER_NO,
       APPN_NO,
       SOURCE,
       LOAD_DTS,
       ETL_JOB_ID
FROM (
         SELECT LNK_STUDYLINK_APPLICATION_COURSE_TO_AMIS_COURSE_APPLICATION_KEY,
                HUB_STUDYLINK_APPLICATION_COURSE_KEY,
                HUB_COURSE_APPLICATION_KEY,
                APPLICATION_ID,
                COURSE_CODE,
                STU_ID,
                SPK_NO,
                SPK_VER_NO,
                APPN_NO,
                SOURCE,
                LOAD_DTS,
                ETL_JOB_ID
         FROM (
                  SELECT MD5(IFNULL(DET.APPLICATION_ID, '') || ',' ||
                             IFNULL(DET.COURSE_CODE, '') || ',' ||
                             IFNULL(DET.STU_ID, '') || ',' ||
                             IFNULL(DET.SPK_NO, 0) || ',' ||
                             IFNULL(DET.SPK_VER_NO, 0) || ',' ||
                             IFNULL(CONCAT(AMIS_APP.APPLICATION_ID, '_', APPLICATION_LINE_ID), ''))
                                                                                      LNK_STUDYLINK_APPLICATION_COURSE_TO_AMIS_COURSE_APPLICATION_KEY,
                         MD5(IFNULL(DET.APPLICATION_ID, '') || ',' ||
                             IFNULL(DET.COURSE_CODE, ''))                             HUB_STUDYLINK_APPLICATION_COURSE_KEY,
                         MD5(IFNULL(DET.STU_ID, '') || ',' ||
                             IFNULL(DET.SPK_NO, 0) || ',' ||
                             IFNULL(DET.SPK_VER_NO, 0) || ',' ||
                             IFNULL(CONCAT(AMIS_APP.APPLICATION_ID, '_', APPLICATION_LINE_ID),
                                    ''))                                              HUB_COURSE_APPLICATION_KEY,
                         DET.APPLICATION_ID,
                         DET.COURSE_CODE,
                         DET.STU_ID,
                         DET.SPK_NO,
                         DET.SPK_VER_NO,
                         CONCAT(AMIS_APP.APPLICATION_ID, '_', APPLICATION_LINE_ID) as APPN_NO,
                         ROW_NUMBER() OVER ( PARTITION BY DET.APPLICATION_ID , DET.COURSE_CODE ORDER BY
                             -- AMIS_APP.ADMSN_CNTR_APP_ID DESC NULLS LAST , AMIS_APP.ADMSN_CNTR_CRS_CD DESC ----NULLS LAST,

                             UN_CENSUS_DT DESC NULLS LAST,
                             CASE CS_SSP.SSP_STTS_CD
                                 WHEN 'ADM' THEN 1
                                 WHEN 'POTC' THEN 2
                                 WHEN 'LOA' THEN 3
                                 WHEN 'PASS' THEN 4
                                 WHEN 'AWOL' THEN 5
                                 WHEN 'OFF' THEN 6
                                 WHEN 'APL' THEN 7
                                 WHEN 'WD' THEN 8
                                 WHEN 'WDE' THEN 9
                                 WHEN 'REPR' THEN 10
                                 ELSE 99
                                 END ASC,
                             CS_SSP.EFFCT_START_DT DESC, CS_SSP.SSP_ATT_NO DESC, CS_SSP.SSP_NO DESC,
                             -- AMIS_APP.APPN_NO DESC,
                             AMIS_APP.LAST_MOD_DATEI DESC,
                             AMIS_APP.LAST_MOD_TIMEI )                                APP_COURSE_RNK,
                         'AMIS-STUDYLINK'                                             SOURCE,
                         CURRENT_TIMESTAMP :: TIMESTAMP_NTZ                           LOAD_DTS,
                         'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ                  ETL_JOB_ID
                  FROM (
                           WITH STUDYLINK_APP_COURSE_MAPPING AS
                                    (
                                        SELECT DF_APPLICATIONDETAILS_APPLICATIONID,
                                               DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID,
                                               DF_APPLICATIONDETAILS_STATUSCODE,
                                               PREFERENCEFIELDS_INTAKE_YEAR,
                                               DF_APPLICATIONDETAILS_APPSTATUSDESCRIPTION,
                                               PREFERENCEFIELDS_INTAKE_UNITNUMBER,
                                               PREFERENCEFIELDS_CUSTOMCOURSECODE,
                                               PREFERENCEFIELDS_COURSECRICOSCODE,
                                               RANK_APP_STATUS_INTAKE_YEAR_SESSION
                                        FROM (
                                                 SELECT *,
                                                        RANK() OVER ( PARTITION BY
                                                            APP_COURSE_DET.DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID ,
                                                            APP_COURSE_DET.PREFERENCEFIELDS_CUSTOMCOURSECODE
                                                            ORDER BY APP_STATUS_ORDER ASC, PREFERENCEFIELDS_INTAKE_YEAR DESC , PREFERENCEFIELDS_INTAKE_UNITNUMBER DESC ) AS RANK_APP_STATUS_INTAKE_YEAR_SESSION
                                                 FROM ( -- FOR A STUDENT AND COURSE COMBINATION GET THE HIGHEST PRIORITY APPLICATION RECORD, THEN THE HIGHEST INTAKE YEAR AND THEN THE HIGHEST INTAKE SESSION
                                                          SELECT SL_APPLICANT.DF_APPLICATIONDETAILS_APPLICATIONID,
                                                                 SL_APPLICANT.DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID,
                                                                 SL_APPLICANT.DF_APPLICATIONDETAILS_STATUSCODE,
                                                                 IFNULL(
                                                                         SL_COURSE.PREFERENCEFIELDS_INTAKE_YEAR,
                                                                         '1900'
                                                                     )   PREFERENCEFIELDS_INTAKE_YEAR,
                                                                 SL_APPLICANT.DF_APPLICATIONDETAILS_APPSTATUSDESCRIPTION,
                                                                 IFNULL(
                                                                         SL_COURSE.PREFERENCEFIELDS_INTAKE_UNITNUMBER,
                                                                         '0'
                                                                     )   PREFERENCEFIELDS_INTAKE_UNITNUMBER,
                                                                 SL_COURSE.PREFERENCEFIELDS_CUSTOMCOURSECODE,
                                                                 SL_COURSE.PREFERENCEFIELDS_COURSECRICOSCODE,
                                                                 CASE
                                                                     WHEN UPPER(SL_APPLICANT.DF_APPLICATIONDETAILS_APPSTATUSDESCRIPTION) IN
                                                                          ('CONDITIONAL ACCEPTANCE PROCESSING',
                                                                           'OFFER ACCEPTED',
                                                                           'ACCEPTED - PAYMENT RECEIVED',
                                                                           'ACCEPTANCE PROCESSING',
                                                                           'ACCEPTANCE, PENDING DEFERRAL',
                                                                           'ACCEPTED - PENDING PAYMENT',
                                                                           'CONDITIONAL ACCEPTANCE, PENDING DEFERRAL',
                                                                           'CONDITIONAL ACCEPTANCE',
                                                                           'ACCEPTANCE')
                                                                         THEN 1
                                                                     WHEN UPPER(SL_APPLICANT.DF_APPLICATIONDETAILS_APPSTATUSDESCRIPTION) IN
                                                                          ('CONDITIONAL OFFER',
                                                                           'FULL OFFER WITH CONDITIONS')
                                                                         THEN 2
                                                                     WHEN UPPER(SL_APPLICANT.DF_APPLICATIONDETAILS_APPSTATUSDESCRIPTION) IN
                                                                          ('SUBMITTED',
                                                                           'ASSESSING - PENDING INTERVIEW',
                                                                           'ASSESSING - RETURNED FROM SPECIAL APPROVAL',
                                                                           'ASSESSING - REFERRED FOR RPL',
                                                                           'ASSESSING - RETURNED FROM FACULTY',
                                                                           'ASSESSING - RETURNED FROM RPL ASSESSMENT',
                                                                           'ASSESSING - REFERRED FOR SPECIAL APPROVAL',
                                                                           'PENDING ASSESSMENT',
                                                                           'ASSESSING',
                                                                           'ASSESSING - REFERRED TO FACULTY',
                                                                           'QUALIFIED',
                                                                           'ASSESSING - MORE INFO REQUIRED')
                                                                         THEN 3
                                                                     WHEN UPPER(SL_APPLICANT.DF_APPLICATIONDETAILS_APPSTATUSDESCRIPTION) IN
                                                                          ('NEW APPLICATION', 'DEFERRAL REQUESTED',
                                                                           'SUBMITTED - ID ALLOCATED')
                                                                         THEN 4
                                                                     WHEN UPPER(SL_APPLICANT.DF_APPLICATIONDETAILS_APPSTATUSDESCRIPTION) IN
                                                                          ('RECEIVED INTO AMRD',
                                                                           'NOT QUALIFIED',
                                                                           'WITHDRAWN',
                                                                           'INCOMPLETE APPLICATION',
                                                                           'OFFER DECLINED')
                                                                         THEN 5
                                                                     ELSE 6
                                                                     END APP_STATUS_ORDER
                                                          FROM ODS.STUDYLINK.STUDYLINK_APPLICATION_API_LATEST SL_APPLICANT
                                                                   INNER JOIN ODS.STUDYLINK.STUDYLINK_APPLICATION_COURSE_API_LATEST SL_COURSE
                                                                              ON SL_APPLICANT.DF_APPLICATIONDETAILS_APPLICATIONID =
                                                                                 SL_COURSE.DF_APPLICATIONDETAILS_APPLICATIONID
                                                          WHERE UPPER(SL_APPLICANT.DF_APPLICATIONDETAILS_APPSTATUSDESCRIPTION) NOT IN
                                                                ('NEW APPLICATION', 'INCOMPLETE APPLICATION')
                                                            AND UPPER(SL_APPLICANT.DF_APPLICATIONDETAILS_APPGROUP) NOT IN
                                                                (
                                                                 'EX INBOUND',
                                                                 'TEST',
                                                                 'SA - HE',
                                                                 'FOUNDATION (DOMESTIC)',
                                                                 'SA INBOUND',
                                                                 '',
                                                                 'ELC'
                                                                    )
                                                            AND (SL_APPLICANT.DF_APPLICANTDETAILS_FIRSTNAME !=
                                                                 'DANTEST' OR
                                                                 SL_APPLICANT.DF_APPLICANTDETAILS_LASTNAME != 'DANTEST')
                                                      ) APP_COURSE_DET
                                             )
                                        WHERE RANK_APP_STATUS_INTAKE_YEAR_SESSION = 1
                                    ),
                                CRICOS_CODE_DET
                                    AS
                                    (
                                        SELECT STU_APP.STU_ID,
                                               STU_APP.APPN_NO,
                                               CS_SPK_DET.SPK_NO,
                                               CS_SPK_DET.SPK_VER_NO,
                                               MAX(CS_SPK_CORRSPND_CD.CORRSPND_CD) CRICOS_CODE
                                        FROM ODS.AMIS.S1SPK_CORRSPND_CD CS_SPK_CORRSPND_CD
                                                 JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                                                      ON CS_SPK_DET.SPK_NO = CS_SPK_CORRSPND_CD.SPK_NO
                                                          AND CS_SPK_DET.SPK_VER_NO = CS_SPK_CORRSPND_CD.SPK_VER_NO
                                                          AND CS_SPK_DET.SPK_CAT_CD = 'CS'
                                                 JOIN ODS.AMIS.S1STU_APPLICATION STU_APP
                                                      ON CS_SPK_DET.SPK_NO = STU_APP.SPK_NO
                                                          AND CS_SPK_DET.SPK_VER_NO = STU_APP.SPK_VER_NO
                                        WHERE 1 = 1
                                          AND CORRSPND_CD_TYPE = 'CRCOS'
                                          AND CORRSPND_CD != 'DISTANCE'
                                        GROUP BY CS_SPK_DET.SPK_NO
                                                , CS_SPK_DET.SPK_VER_NO,
                                                 STU_APP.STU_ID,
                                                 STU_APP.APPN_NO
                                    )
                                   ,
                                AMIS_COURSE_DET AS (
                                    SELECT *
                                    FROM (
                                             SELECT *,
                                                    RANK()
                                                            OVER (PARTITION BY STU_ID , SPK_NO ORDER BY AMIS_APP_STATUS_RANK ASC, AMIS_COURSE_STATUS_RANK ASC,
                                                                EFFCT_START_DT DESC , APPN_NO DESC, SPK_VER_NO DESC) AMIS_RANK
                                             FROM (
                                                      SELECT AMIS_STU_APP.SPK_NO,
                                                             AMIS_STU_APP.SPK_VER_NO,
                                                             AMIS_STU_APP.STU_ID,
                                                             STU_DET.CONSOL_TO_STU_ID,
                                                             AMIS_STU_APP.APPN_NO,
                                                             IFF(AMIS_SPK_DET.SPK_CD = 'GD-TRANINT', 'GD-TRANINT.K',
                                                                 AMIS_SPK_DET.SPK_CD) SPK_CD,
                                                             AMIS_SPK_DET.SPK_SHORT_TITLE,
                                                             AMIS_STU_APP.ADMSN_CNTR_APP_ID,
                                                             AMIS_STU_APP.APPN_STTS_CD,
                                                             AMIS_STU_APP.APPN_DT,
                                                             CASE
                                                                 WHEN AMIS_STU_APP.APPN_STTS_CD IN ('AD')
                                                                     THEN 1
                                                                 WHEN AMIS_STU_APP.APPN_STTS_CD IN ('QC', 'QL')
                                                                     THEN 2
                                                                 WHEN AMIS_STU_APP.APPN_STTS_CD IN ('UN')
                                                                     THEN 3
                                                                 WHEN AMIS_STU_APP.APPN_STTS_CD IN ('NQ', 'IN', 'US')
                                                                     THEN 4
                                                                 WHEN AMIS_STU_APP.APPN_STTS_CD IN ('NA', 'WD')
                                                                     THEN 5
                                                                 ELSE 6
                                                                 END                  AMIS_APP_STATUS_RANK,
                                                             CS_SSP.SSP_STTS_CD,
                                                             CASE
                                                                 WHEN CS_SSP.SSP_STTS_CD IN ('ADM', 'POTC')
                                                                     THEN 1
                                                                 WHEN AMIS_STU_APP.APPN_STTS_CD IN ('LOA')
                                                                     THEN 2
                                                                 WHEN AMIS_STU_APP.APPN_STTS_CD IN ('PASS')
                                                                     THEN 3
                                                                 ELSE 6
                                                                 END                  AMIS_COURSE_STATUS_RANK,
                                                             CS_SSP.EFFCT_START_DT
                                                      FROM ODS.AMIS.S1STU_APPLICATION AMIS_STU_APP
                                                               INNER JOIN ODS.AMIS.S1SPK_DET AMIS_SPK_DET
                                                                          ON AMIS_STU_APP.SPK_NO = AMIS_SPK_DET.SPK_NO
                                                                              AND
                                                                             AMIS_STU_APP.SPK_VER_NO =
                                                                             AMIS_SPK_DET.SPK_VER_NO
                                                               JOIN ODS.AMIS.S1STU_DET STU_DET
                                                                    ON AMIS_STU_APP.STU_ID = STU_DET.STU_ID
                                                               JOIN ODS.AMIS.S1SSP_STU_SPK CS_SSP
                                                                    ON CS_SSP.STU_ID = AMIS_STU_APP.STU_ID
                                                                        AND CS_SSP.SPK_NO = AMIS_STU_APP.SPK_NO
                                                                        AND CS_SSP.SPK_VER_NO = AMIS_STU_APP.SPK_VER_NO
                                                                        AND CS_SSP.APPN_NO = AMIS_STU_APP.APPN_NO
                                                      WHERE AMIS_SPK_DET.SPK_CAT_CD = 'CS'
                                                  )
                                         )
                                    WHERE AMIS_RANK = 1)
                           SELECT SL_AMIS_MAP.DF_APPLICATIONDETAILS_APPLICATIONID APPLICATION_ID,
                                  SL_AMIS_MAP.PREFERENCEFIELDS_CUSTOMCOURSECODE   COURSE_CODE,
                                  SL_AMIS_MAP.STU_ID,
                                  SL_AMIS_MAP.SPK_NO,
                                  SL_AMIS_MAP.SPK_VER_NO,
                                  SL_AMIS_MAP.APPN_NO
                           FROM (
                                    -- MATCH ON APPLICATION ID AND (COURSE CODE OR CRICOS CODE)
                                    SELECT STUDYLINK_APP_COURSE_MAPPING.DF_APPLICATIONDETAILS_APPLICATIONID,
                                           STUDYLINK_APP_COURSE_MAPPING.PREFERENCEFIELDS_CUSTOMCOURSECODE,
                                           AMIS_COURSE_DET.STU_ID,
                                           AMIS_COURSE_DET.SPK_NO,
                                           AMIS_COURSE_DET.SPK_VER_NO,
                                           AMIS_COURSE_DET.APPN_NO
                                    FROM CRICOS_CODE_DET
                                             JOIN AMIS_COURSE_DET
                                                  ON CRICOS_CODE_DET.SPK_NO = AMIS_COURSE_DET.SPK_NO
                                                      AND CRICOS_CODE_DET.SPK_VER_NO = AMIS_COURSE_DET.SPK_VER_NO
                                                      AND CRICOS_CODE_DET.STU_ID = AMIS_COURSE_DET.STU_ID
                                                      AND CRICOS_CODE_DET.APPN_NO = AMIS_COURSE_DET.APPN_NO
                                             JOIN STUDYLINK_APP_COURSE_MAPPING
                                                  ON STUDYLINK_APP_COURSE_MAPPING.DF_APPLICATIONDETAILS_APPLICATIONID =
                                                     AMIS_COURSE_DET.ADMSN_CNTR_APP_ID
                                                      AND
                                                     (STUDYLINK_APP_COURSE_MAPPING.PREFERENCEFIELDS_COURSECRICOSCODE =
                                                      CRICOS_CODE_DET.CRICOS_CODE OR
                                                      STUDYLINK_APP_COURSE_MAPPING.PREFERENCEFIELDS_CUSTOMCOURSECODE
                                                          = AMIS_COURSE_DET.SPK_CD)
                                    UNION ALL
                                    -- MATCH ON CRICOS CODE IF THERE IS NO MATCH ON APP ID
                                    SELECT STUDYLINK_APP_COURSE_MAPPING.DF_APPLICATIONDETAILS_APPLICATIONID,
                                           STUDYLINK_APP_COURSE_MAPPING.PREFERENCEFIELDS_CUSTOMCOURSECODE,
                                           AMIS_COURSE_DET.STU_ID,
                                           AMIS_COURSE_DET.SPK_NO,
                                           AMIS_COURSE_DET.SPK_VER_NO,
                                           AMIS_COURSE_DET.APPN_NO
                                    FROM CRICOS_CODE_DET
                                             JOIN AMIS_COURSE_DET
                                                  ON CRICOS_CODE_DET.SPK_NO = AMIS_COURSE_DET.SPK_NO
                                                      AND CRICOS_CODE_DET.SPK_VER_NO = AMIS_COURSE_DET.SPK_VER_NO
                                                      AND CRICOS_CODE_DET.STU_ID = AMIS_COURSE_DET.STU_ID
                                                      AND CRICOS_CODE_DET.APPN_NO = AMIS_COURSE_DET.APPN_NO
                                             JOIN STUDYLINK_APP_COURSE_MAPPING
                                                  ON STUDYLINK_APP_COURSE_MAPPING.PREFERENCEFIELDS_COURSECRICOSCODE =
                                                     CRICOS_CODE_DET.CRICOS_CODE
                                                      AND
                                                     STUDYLINK_APP_COURSE_MAPPING.DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID IN
                                                     (AMIS_COURSE_DET.STU_ID, AMIS_COURSE_DET.CONSOL_TO_STU_ID)
                                    WHERE STUDYLINK_APP_COURSE_MAPPING.DF_APPLICATIONDETAILS_APPLICATIONID !=
                                          AMIS_COURSE_DET.ADMSN_CNTR_APP_ID
                                    UNION ALL
                                    -- MATCH ON COURSE CODE IF NO MATCH ON APP_ID AND CRICOS_CODE
                                    SELECT STUDYLINK_APP_COURSE_MAPPING.DF_APPLICATIONDETAILS_APPLICATIONID,
                                           STUDYLINK_APP_COURSE_MAPPING.PREFERENCEFIELDS_CUSTOMCOURSECODE,
                                           AMIS_COURSE_DET.STU_ID,
                                           AMIS_COURSE_DET.SPK_NO,
                                           AMIS_COURSE_DET.SPK_VER_NO,
                                           AMIS_COURSE_DET.APPN_NO
                                    FROM AMIS_COURSE_DET
                                             JOIN STUDYLINK_APP_COURSE_MAPPING
                                                  ON STUDYLINK_APP_COURSE_MAPPING.DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID IN
                                                     (AMIS_COURSE_DET.STU_ID, AMIS_COURSE_DET.CONSOL_TO_STU_ID)
                                                      AND AMIS_COURSE_DET.SPK_CD =
                                                          STUDYLINK_APP_COURSE_MAPPING.PREFERENCEFIELDS_CUSTOMCOURSECODE
                                             LEFT JOIN CRICOS_CODE_DET
                                                       ON AMIS_COURSE_DET.SPK_NO = CRICOS_CODE_DET.SPK_NO
                                                           AND AMIS_COURSE_DET.SPK_VER_NO = CRICOS_CODE_DET.SPK_VER_NO
                                                           AND CRICOS_CODE_DET.STU_ID = AMIS_COURSE_DET.STU_ID
                                                           AND CRICOS_CODE_DET.APPN_NO = AMIS_COURSE_DET.APPN_NO
                                    WHERE STUDYLINK_APP_COURSE_MAPPING.DF_APPLICATIONDETAILS_APPLICATIONID !=
                                          AMIS_COURSE_DET.ADMSN_CNTR_APP_ID
                                      AND STUDYLINK_APP_COURSE_MAPPING.PREFERENCEFIELDS_COURSECRICOSCODE !=
                                          IFNULL(CRICOS_CODE_DET.CRICOS_CODE, 'NOTFOUND')
                                ) SL_AMIS_MAP
                                    JOIN ODS.AMIS.S1STU_DET STU_DET
                                         ON SL_AMIS_MAP.STU_ID = STU_DET.STU_ID
                                             AND STU_DET.STU_CONSOL_FG = 'N') DET

                           INNER JOIN ODS.AMIS.S1APP_STUDY AMIS_APP
                                      ON DET.STU_ID = AMIS_APP.STU_ID
                                          --AND DET.APPN_NO = AMIS_APP.APPN_NO
                                          AND DET.SPK_VER_NO = AMIS_APP.SPK_VER_NO
                                          AND DET.SPK_NO = AMIS_APP.SPK_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SSP_STU_SPK CS_SSP
                                           ON AMIS_APP.SPK_NO = CS_SSP.APPN_SPK_NO
                                               AND AMIS_APP.SPK_VER_NO = CS_SSP.APPN_VER_NO
                                               -- AND AMIS_APP.APPN_NO = CS_SSP.APPN_NO
                                               AND AMIS_APP.STU_ID = CS_SSP.STU_ID
                           LEFT OUTER JOIN (
                      SELECT UN_SSP.PARENT_SSP_NO,
                             MAX(UN_SSP.CENSUS_DT) UN_CENSUS_DT
                      FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
                               JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
                                    ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                                        AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                                        AND UN_SPK_DET.SPK_CAT_CD = 'UN'
                      WHERE YEAR(UN_SSP.CENSUS_DT) > 1900
                      GROUP BY UN_SSP.PARENT_SSP_NO
                  ) UN_CENSUS
                                           ON UN_CENSUS.PARENT_SSP_NO = CS_SSP.SSP_NO
              ) LNK_DET
         WHERE APP_COURSE_RNK = 1
           AND NOT EXISTS(
                 SELECT NULL
                 FROM DATA_VAULT.CORE.LNK_STUDYLINK_APPLICATION_COURSE_TO_AMIS_COURSE_APPLICATION L
                 WHERE L.HUB_STUDYLINK_APPLICATION_COURSE_KEY = MD5(
                             IFNULL(LNK_DET.APPLICATION_ID, '') || ',' ||
                             IFNULL(LNK_DET.COURSE_CODE, ''))
                   AND L.HUB_COURSE_APPLICATION_KEY = MD5(
                             IFNULL(LNK_DET.STU_ID, '') || ',' ||
                             IFNULL(LNK_DET.SPK_NO, 0) || ',' ||
                             IFNULL(LNK_DET.SPK_VER_NO, 0) || ',' ||
                             IFNULL(LNK_DET.APPN_NO, ''))
             )
     );