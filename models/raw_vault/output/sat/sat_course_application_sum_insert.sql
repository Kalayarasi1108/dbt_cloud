INSERT INTO DATA_VAULT.CORE.SAT_COURSE_APPLICATION_SUM (SAT_COURSE_APPLICATION_SUM_SK,
                                                        HUB_COURSE_APPLICATION_KEY,
                                                        SOURCE,
                                                        LOAD_DTS,
                                                        ETL_JOB_ID,
                                                        HASH_MD5,
                                                        STUDENT_ID,
                                                        CS_SPK_NO,
                                                        CS_SPK_VER_NO,
                                                        APPN_NO,
                                                        APPLICATION_TYPE_CODE,
                                                        APPLICATION_TYPE,
                                                        ADMISSION_CENTRE_COURSE_CODE,
                                                        ADMISSION_CENTRE_APPLICANT_ID,
                                                        LIABILITY_CATEGORY_CODE,
                                                        LIABILITY_CATEGORY,
                                                        LOAD_CATEGORY_CODE,
                                                        LOAD_CATEGORY,
                                                        ATTENDANCE_MODE_CODE,
                                                        ATTENDANCE_MODE,
                                                        APPLICATION_DATE,
                                                        APPLICATION_STAGE_CODE,
                                                        APPLICATION_STAGE,
                                                        APPLICATION_STATUS_CODE,
                                                        APPLICATION_STATUS,
                                                        OWNING_ORG_UNIT_CODE,
                                                        OWNING_ORG_UNIT_NAME,
                                                        APPLICATION_PREFERENCE_NUMBER,
                                                        OFFER_PREFERENCE_NUMBER,
                                                        OFFER_ROUND,
                                                        COURSE_CODE,
                                                        COURSE_FULL_TITLE,
                                                        HDR_COURSE,
                                                        COURSE_CATEGORY_TYPE_CODE,
                                                        COURSE_CATEGORY_TYPE,
                                                        OFFER_STATUS_CODE,
                                                        OFFER_STATUS,
                                                        OFFER_OFFERED_DATE,
                                                        OFFER_ACCEPTED_DATE,
                                                        OFFER_DEFERRED_DATE,
                                                        OFFER_DECLINED_DATE,
                                                        OFFER_ADMITTED_DATE,
                                                        OFFER_EXPIRED_DATE,
                                                        OFFER_LAPSED_DATE,
                                                        OFFER_RESCINDED_DATE,
                                                        OFFER_UNOFFERED_DATE,
                                                        OFFER_ACCEPTANCE,
                                                        OFFER_STATUS_EFFECTIVE_DATE,
                                                        COURSE_ADMISSION_STAGE_CODE,
                                                        COURSE_ADMISSION_STAGE,
                                                        COURSE_ADMISSION_ADMITTED_DATE,
                                                        COURSE_ADMISSION_STATUS_CODE,
                                                        COURSE_ADMISSION_STATUS,
                                                        APPLICATION_PATH,
                                                        CS_SSP_NO,
                                                        FIRST_UNIT_ENROLLED_DATE,
                                                        IS_DELETED,
                                                        COURSE_TYPE_CODE,
                                                        COURSE_TYPE_DESCRIPTION,
                                                        PRIMARY_FIELD_OF_EDUCATION_CODE,
                                                        PRIMARY_FIELD_OF_EDUCATION_DESC,
                                                        SECONDARY_FIELD_OF_EDUCATION_CODE,
                                                        SECONDARY_FIELD_OF_EDUCATION_DESC,
                                                        CRICOS_CODE,
                                                        FIELD_OF_EDUCATION_6_DIGIT_CODE,
                                                        FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
                                                        FIELD_OF_EDUCATION_4_DIGIT_CODE,
                                                        FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
                                                        FIELD_OF_EDUCATION_2_DIGIT_CODE,
                                                        FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
                                                        SUBMISSION_DT,
                                                        SUBMISSION_METHOD_CD,
                                                        SUBMISSION_METHOD)


WITH SAT_APPLICATION_SUM AS (
    SELECT *
    FROM (
             SELECT HUB_COURSE_APPLICATION_KEY,
                    OFFER_STATUS_CODE,
                    MD5(CONCAT(HUB_COURSE_APPLICATION_KEY, '_', OFFER_STATUS_CODE)) AS               OFFER_HASH,
                    HASH_MD5,
                    LOAD_DTS,
                    LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_APPLICATION_KEY ORDER BY LOAD_DTS ) EFFECTIVE_END_DTS
             FROM DATA_VAULT.CORE.SAT_COURSE_APPLICATION_SUM
         ) AS A
    WHERE EFFECTIVE_END_DTS IS NULL
),
     S1APP_APPLICATION AS (
         SELECT L.STU_ID,
                S.SPK_NO,
                S.SPK_VER_NO,
                CONCAT(L.APPLICATION_ID, '_', L.APPLICATION_LINE_ID)            AS APPN_NO,
                RELEASED_DT,
                OFFER_STATUS_CD,
                L.CRDATEI,
                SUBMISSION_METHOD_CD,
                L.APPLICATION_TYPE_CD,
                ATTENDANCE_MODE_CD,
                SUBMISSION_DT,
                APPLICATION_STATUS_CD,
                PREFERENCE_NO,
                LAPSE_DT,
                S.SSP_NO,
                APP.ADMSN_CNTR_CRS_CD,
                APP.ADMSN_CNTR_APP_ID,
                LIABILITY_CATEGORY_CD,
                LOAD_CATEGORY_CD,

                OFFER_OUTCOME_CD                                                as OFFER_STATUS_CODE,
                APPLICATION_STATUS_CD                                           AS APPLICATION_STATUS,
                APPLICATION_STATUS_CD                                           as APPLICATION_STATUS_CODE,
                APP.OWNING_ORG_UNIT_CD                                          AS OWN_ORG_UNIT_CD,
                app.OFFER_ROUND                                                    OFFER_ROUND,
                APPN_PREF_NO::VARCHAR(20)                                       AS APPLICATION_PREFERENCE_NUMBER,
                o.last_mod_datei                                                as last_offer_date,

                IFF(SUBMISSION_METHOD_CD IN ('D', 'OS', 'SP'), O.CRDATEI, NULL) as OFFER_OFFERED_DATE,


                S.CRDATEI                                                       as ADMITTED_DATE,
                S.AVAIL_KEY_NO,
                A.CRDATEI                                                       as APPLICATION_DT
         FROM ODS.AMIS.S1APP_APPLICATION AS A
                  INNER JOIN ODS.AMIS.S1APP_APPLICATION_LINE AS L
                             ON A.APPLICATION_ID = L.APPLICATION_ID
                  INNER JOIN ODS.AMIS.S1APP_STUDY AS S
                             ON L.APPLICATION_ID = S.APPLICATION_ID
                                 AND S.APPLICATION_LINE_ID = L.APPLICATION_LINE_ID
                  LEFT JOIN ODS.AMIS.S1APP_OFFER AS O
                            ON O.APPLICATION_ID = L.APPLICATION_ID
                                AND L.APPLICATION_LINE_ID = O.APPLICATION_LINE_ID
                  LEFT JOIN ODS.AMIS.S1STU_APPLICATION AS APP
                            ON APP.APPLICATION_ID = L.APPLICATION_ID
                                AND APP.APPLICATION_LINE_ID = L.APPLICATION_LINE_ID
         -- LEFT JOIN ODS.AMIS.S1SPK_EX as E on e.spk_no = s.spk_no and e.spk_ver_no = s.spk_ver_no
     ),
     SAT_APP_OFFER AS (
         SELECT S1APP.*,
                NULL              AS OFFER_ADMITTED_DATE,
                NULL              AS OFFER_EXPIRED_DATE,
                NULL              AS OFFER_UNOFFERED_DATE,
                NULL              AS OFFER_STATUS_EFFECTIVE_DATE,
                CASE
                    WHEN (SAT_APP.HUB_COURSE_APPLICATION_KEY IS NULL OR SAT_APP.OFFER_HASH != MD5(CONCAT(
                                IFNULL(S1APP.STU_ID, '') || ',' ||
                                IFNULL(S1APP.SPK_NO, 0) || ',' ||
                                IFNULL(S1APP.SPK_VER_NO, 0) || ',' ||
                                IFNULL(S1APP.APPN_NO, ''), '_', S1APP.OFFER_STATUS_CODE)
                        )
                             )
                        AND S1APP.OFFER_STATUS_CODE = '$ACC'
                        AND S1APP.SUBMISSION_METHOD_CD NOT IN ('SI', 'AP', 'RPA', 'SL')
                        THEN LAST_OFFER_DATE
                    ELSE NULL END AS OFFER_ACCEPTED_DATE,
                CASE
                    WHEN (SAT_APP.HUB_COURSE_APPLICATION_KEY IS NULL OR SAT_APP.OFFER_HASH != MD5(CONCAT(
                                IFNULL(S1APP.STU_ID, '') || ',' ||
                                IFNULL(S1APP.SPK_NO, 0) || ',' ||
                                IFNULL(S1APP.SPK_VER_NO, 0) || ',' ||
                                IFNULL(S1APP.APPN_NO, ''), '_', S1APP.OFFER_STATUS_CODE)
                        )
                             ) AND S1APP.OFFER_STATUS_CODE = '$DEC'
                        THEN LAST_OFFER_DATE
                    ELSE NULL END AS OFFER_DECLINED_DATE,
                CASE
                    WHEN (SAT_APP.HUB_COURSE_APPLICATION_KEY IS NULL OR SAT_APP.OFFER_HASH != MD5(CONCAT(
                                IFNULL(S1APP.STU_ID, '') || ',' ||
                                IFNULL(S1APP.SPK_NO, 0) || ',' ||
                                IFNULL(S1APP.SPK_VER_NO, 0) || ',' ||
                                IFNULL(S1APP.APPN_NO, ''), '_', S1APP.OFFER_STATUS_CODE)
                        )
                             ) AND S1APP.OFFER_STATUS_CODE = '$RES'
                        THEN LAST_OFFER_DATE
                    ELSE NULL END AS OFFER_RESCINDED_DATE,

                CASE
                    WHEN (SAT_APP.HUB_COURSE_APPLICATION_KEY IS NULL OR SAT_APP.OFFER_HASH != MD5(CONCAT(
                                IFNULL(S1APP.STU_ID, '') || ',' ||
                                IFNULL(S1APP.SPK_NO, 0) || ',' ||
                                IFNULL(S1APP.SPK_VER_NO, 0) || ',' ||
                                IFNULL(S1APP.APPN_NO, ''), '_', S1APP.OFFER_STATUS_CODE)
                        )
                             ) AND S1APP.OFFER_STATUS_CODE = '$DEF'
                        THEN LAST_OFFER_DATE
                    ELSE NULL END AS OFFER_DEFERRED_DATE,
                CASE
                    WHEN (SAT_APP.HUB_COURSE_APPLICATION_KEY IS NULL OR SAT_APP.OFFER_HASH != MD5(CONCAT(
                                IFNULL(S1APP.STU_ID, '') || ',' ||
                                IFNULL(S1APP.SPK_NO, 0) || ',' ||
                                IFNULL(S1APP.SPK_VER_NO, 0) || ',' ||
                                IFNULL(S1APP.APPN_NO, ''), '_', S1APP.OFFER_STATUS_CODE)
                        )
                             ) AND S1APP.OFFER_STATUS_CODE = '$LAP'
                        THEN LAST_OFFER_DATE
                    ELSE NULL END AS OFFER_LAPSED_DATE
         FROM S1APP_APPLICATION S1APP
                  LEFT JOIN SAT_APPLICATION_SUM SAT_APP ON SAT_APP.HUB_COURSE_APPLICATION_KEY = MD5(
                     IFNULL(S1APP.STU_ID, '') || ',' ||
                     IFNULL(S1APP.SPK_NO, 0) || ',' ||
                     IFNULL(S1APP.SPK_VER_NO, 0) || ',' ||
                     IFNULL(S1APP.APPN_NO, '')
             )
     ),
     AVAIL_ORG AS (
         SELECT SPK_NO,
                SPK_VER_NO,
                AVAIL_KEY_NO,
                ORG_UNIT_CD,
                ROW_NUMBER() OVER (PARTITION BY SPK_NO, SPK_VER_NO, AVAIL_KEY_NO ORDER BY AVAIL_KEY_NO DESC) AS RN
         FROM ODS.AMIS.S1SPK_AVAIL_ORG
     ),
     ORG_UNIT AS (
         SELECT ORG_UNIT_CD,
                ORG_UNIT_NM,
                ORG_UNIT_SHORT_NM,
                ORG_UNIT_TYPE_CD,
                ROW_NUMBER() OVER (PARTITION BY ORG_UNIT_CD ORDER BY EFFCT_DT DESC) RN
         FROM ODS.AMIS.S1ORG_UNIT
     ),
     CS_HIST AS (
         SELECT SSP_NO,
                MIN(EFFCT_START_DT) ADMITTED_DATE
         FROM ODS.AMIS.S1SSP_STTS_HIST
         WHERE SSP_STTS_CD = 'ADM'
         GROUP BY SSP_NO
     ),
     UN_FIRST_ENR AS (
         SELECT UN_SSP.PARENT_SSP_NO     CS_SSP,
                MIN(HIST.EFFCT_START_DT) FIRST_ENROL_DATE
         FROM ODS.AMIS.S1SSP_STTS_HIST HIST
                  JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP ON UN_SSP.SSP_NO = HIST.SSP_NO
             AND TO_CHAR(UN_SSP.CENSUS_DT, 'YYYY') > '1900'
         WHERE HIST.SSP_STTS_CD = 'ENR'
         GROUP BY UN_SSP.PARENT_SSP_NO
     ),
     FOE AS (
         SELECT A.SPK_NO,
                A.SPK_VER_NO,
                MAX(IFF(A.PRIM_SECONDARY_CD = 'P', A.FOE_TRLN_CD, NULL)) PRIMARY_FIELD_OF_EDUCATION_CODE,
                MAX(IFF(A.PRIM_SECONDARY_CD = 'P', A.FOE_DESC, NULL))    PRIMARY_FIELD_OF_EDUCATION_DESC,
                MAX(IFF(A.PRIM_SECONDARY_CD = 'S', A.FOE_TRLN_CD, NULL)) SECONDARY_FIELD_OF_EDUCATION_CODE,
                MAX(IFF(A.PRIM_SECONDARY_CD = 'S', A.FOE_DESC, NULL))    SECONDARY_FIELD_OF_EDUCATION_DESC
         FROM (
                  SELECT CS_SPK_DET.SPK_NO,
                         CS_SPK_DET.SPK_VER_NO,
                         CS_SPK_FOE.FOE_CD,
                         CS_SPK_FOE.PRIM_SECONDARY_CD,
                         FOE_DET.FOE_TRLN_CD,
                         FOE_DET.FOE_DESC
                  FROM ODS.AMIS.S1SPK_DET CS_SPK_DET
                           JOIN ODS.AMIS.S1SPK_FOE CS_SPK_FOE
                                ON CS_SPK_FOE.SPK_NO = CS_SPK_DET.SPK_NO
                                    AND CS_SPK_FOE.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
                           JOIN ODS.AMIS.S1FOE_DET FOE_DET
                                ON FOE_DET.FOE_CD = CS_SPK_FOE.FOE_CD
                  WHERE CS_SPK_DET.SPK_CAT_CD = 'CS'
              ) A
         GROUP BY A.SPK_NO,
                  A.SPK_VER_NO
     ),
     CIRCOS_COURSE AS (
         SELECT CS_SPK_DET.SPK_NO,
                CS_SPK_DET.SPK_VER_NO,
                MAX(CS_SPK_CORRSPND_CD.CORRSPND_CD) CORRSPND_CD
         FROM ODS.AMIS.S1SPK_CORRSPND_CD CS_SPK_CORRSPND_CD
                  JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET ON CS_SPK_DET.SPK_NO = CS_SPK_CORRSPND_CD.SPK_NO
             AND CS_SPK_DET.SPK_VER_NO = CS_SPK_CORRSPND_CD.SPK_VER_NO
             AND CS_SPK_DET.SPK_CAT_CD = 'CS'
         WHERE 1 = 1
           AND CORRSPND_CD_TYPE = 'CRCOS'
           AND CORRSPND_CD != 'DISTANCE'
         GROUP BY CS_SPK_DET.SPK_NO,
                  CS_SPK_DET.SPK_VER_NO
     ),
     CS_AVAIL_ORG AS (
         SELECT SPK_NO,
                SPK_VER_NO,
                AVAIL_KEY_NO,
                ORG_UNIT_CD,
                ROW_NUMBER() OVER (PARTITION BY SPK_NO, SPK_VER_NO, AVAIL_KEY_NO ORDER BY AVAIL_KEY_NO DESC) AS RN
         FROM ODS.AMIS.S1SPK_AVAIL_ORG
         WHERE RESP_CAT_CD = 'O'
     ),
     NEW_APPLICATION AS (
         SELECT S.STU_ID,
                S.SSP_NO,
                S.SPK_NO,
                S.SPK_VER_NO,
                CONCAT(L.APPLICATION_ID, '_', L.APPLICATION_LINE_ID) AS APPN_NO,
                CS_AVAIL_ORG.ORG_UNIT_CD                                OWNING_ORG_UNIT_CD,
                A.SUBMISSION_METHOD_CD

         FROM ODS.AMIS.S1APP_APPLICATION AS A
                  INNER JOIN ODS.AMIS.S1APP_APPLICATION_LINE AS L ON A.APPLICATION_ID = L.APPLICATION_ID
                  INNER JOIN ODS.AMIS.S1APP_STUDY AS S
                             ON L.APPLICATION_ID = S.APPLICATION_ID AND S.APPLICATION_LINE_ID = L.APPLICATION_LINE_ID
                  INNER JOIN ODS.AMIS.S1SPK_AVAIL_DET AVAIL_DET
                             ON AVAIL_DET.AVAIL_KEY_NO = S.AVAIL_KEY_NO
                  LEFT OUTER JOIN CS_AVAIL_ORG
                                  ON CS_AVAIL_ORG.SPK_NO = S.SPK_NO AND
                                     CS_AVAIL_ORG.SPK_VER_NO = S.SPK_VER_NO
                                      AND CS_AVAIL_ORG.AVAIL_KEY_NO = AVAIL_DET.AVAIL_KEY_NO
                                      AND CS_AVAIL_ORG.RN = 1
     ),
     APPLICATION_PATH AS
         (
             SELECT CS_SSP.SSP_NO,
                    CS_SSP.STU_ID,
                    CASE
                        WHEN CS_SSP.SPRD_CD LIKE 'OLA%' OR
                             CS_SSP.SPRD_CD LIKE 'OUA%' THEN 'OUA'
                        WHEN APPLICATION.OWNING_ORG_UNIT_CD = '8245' THEN 'StudyLink'
                        WHEN SPK_CAT_TYPE_CD IN ('100', '110', '122', '125', '130', '132', '170', '178') AND
                             SUBMISSION_METHOD_CD NOT IN ('SL', 'SI', 'RPA', 'AP') THEN 'HDRO'
                        WHEN SUBMISSION_METHOD_CD IS NULL THEN 'Not Entered'
                        ELSE DECODE(SUBMISSION_METHOD_CD, 'SP', 'HDRO', 'OS', 'Direct', 'D', 'Direct', 'RPA',
                                    'StudyLink',
                                    'AP',
                                    'StudyLink', 'SL', 'StudyLink', 'SI', 'StudyLink', 'A', 'UAC', 'C', 'UAC', 'DU',
                                    'UAC',
                                    'P',
                                    'UAC', 'PO', 'UAC', 'U', 'UAC', 'AASN', 'Other', 'ADJ', 'Other', 'UP', 'Other',
                                    'VP',
                                    'Other',
                                    'Other')
                        END APPLICATION_PATH
             FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                      JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                           ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                               AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
                      JOIN NEW_APPLICATION APPLICATION
                           ON APPLICATION.STU_ID = CS_SSP.STU_ID
                               AND APPLICATION.SSP_NO = CS_SSP.SSP_NO
             WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
         ),
     SAT_COURSE_APPLICATION_SUM_INSERT AS (
         SELECT MD5(IFNULL(CTE_3.STU_ID, '') || ',' ||
                    IFNULL(CTE_3.SPK_NO, 0) || ',' ||
                    IFNULL(CTE_3.SPK_VER_NO, 0) || ',' ||
                    IFNULL(CTE_3.APPN_NO, '')
                    )                                                                     HUB_COURSE_APPLICATION_KEY,
                CTE_3.STU_ID                                                              STUDENT_ID,
                CTE_3.SPK_NO                                                              CS_SPK_NO,
                CTE_3.SPK_VER_NO                                                          CS_SPK_VER_NO,
                CTE_3.APPN_NO                                                             APPN_NO,
                CTE_3.OFFER_ACCEPTED_DATE                                                 OFFER_ACCEPTED_DATE,
                CTE_3.OFFER_OFFERED_DATE                                                  OFFER_OFFERED_DATE,
                CTE_3.OFFER_RESCINDED_DATE                                                OFFER_RESCINDED_DATE,
                CTE_3.OFFER_LAPSED_DATE                                                   OFFER_LAPSED_DATE,
                CTE_3.OFFER_DECLINED_DATE                                                 OFFER_DECLINED_DATE,
                CTE_3.OFFER_DEFERRED_DATE                                                 OFFER_DEFERRED_DATE,
                CTE_3.OFFER_ADMITTED_DATE                                                 OFFER_ADMITTED_DATE,
                CTE_3.OFFER_EXPIRED_DATE                                                  OFFER_EXPIRED_DATE,
                CTE_3.OFFER_UNOFFERED_DATE                                                OFFER_UNOFFERED_DATE,
                CS_SPK_DET.SPK_CD                                                         COURSE_CODE,
                APPN_TYPE_CODE.CODE_DESCR                                                 APPLICATION_TYPE,
                CTE_3.SUBMISSION_METHOD_CD                                                APPLICATION_TYPE_CODE,
                CTE_3.ADMSN_CNTR_CRS_CD                                                   ADMISSION_CENTRE_COURSE_CODE,
                CTE_3.ADMSN_CNTR_APP_ID                                                   ADMISSION_CENTRE_APPLICANT_ID,
                CTE_3.LIABILITY_CATEGORY_CD                                               LIABILITY_CATEGORY_CODE,
                CD.CODE_DESCR                                                             LIABILITY_CATEGORY,
                LOAD_CAT_CODE.CODE_DESCR                                                  LOAD_CATEGORY,
                CTE_3.LOAD_CATEGORY_CD                                                    LOAD_CATEGORY_CODE,
                CTE_3.ATTENDANCE_MODE_CD                                                  ATTENDANCE_MODE_CODE,
                ATTNDC_MODE_CODE.CODE_DESCR                                               ATTENDANCE_MODE,
                --SUBMISSION_DT AS APPLICATION_DATE,
                CTE_3.APPLICATION_DT                                                      APPLICATION_DATE,
                SSP_STG_CODE.CODE_DESCR                                                   APPLICATION_STAGE,
                SSP_STG_CODE.CODE_ID                                                      APPLICATION_STAGE_CODE,
                --APPN_STTS_CD AS APPLICATION_STATUS_CODE,
                --JOIN IS EMPTY 'COMPLETE' - CODE_ID
                --APPN_STTS_CODE.CODE_DESCR AS APPLICATION_STATUS,
                X.ORG_UNIT_CD                                                             OWNING_ORG_UNIT_CODE,
                ORG_UNIT.ORG_UNIT_NM                                                      OWNING_ORG_UNIT_NAME,
                CTE_3.APPLICATION_STATUS_CODE                                             APPLICATION_STATUS_CODE,
                CTE_3.APPLICATION_STATUS                                                  APPLICATION_STATUS,
                CTE_3.PREFERENCE_NO                                                       OFFER_PREFERENCE_NUMBER,
                CTE_3.APPLICATION_PREFERENCE_NUMBER                                       APPLICATION_PREFERENCE_NUMBER,
                CTE_3.OFFER_ROUND                                                         OFFER_ROUND,
                CS_SPK_DET.SPK_FULL_TITLE                                                 COURSE_FULL_TITLE,
                IFF(CS_SPK_DET.SPK_CAT_TYPE_CD IN ('110', '130', '178', '122'), 'Y', 'N') HDR_COURSE,
                CS_SPK_DET.SPK_CAT_TYPE_CD                                                COURSE_CATEGORY_TYPE_CODE,
                CAT_TYPE.SPK_CAT_TYPE_DESC                                                COURSE_CATEGORY_TYPE,
                CTE_3.OFFER_STATUS_CODE                                                   OFFER_STATUS_CODE,
                OFFER_OUTCOME.CODE_DESCR                                                  OFFER_STATUS,
                NULL                                                                      OFFER_ACCEPTANCE,
                NULL                                                                      OFFER_STATUS_EFFECTIVE_DATE,
                P.SSP_STG_CD                                                              COURSE_ADMISSION_STAGE_CODE,
                SSP_STG_CODE.CODE_DESCR                                                   COURSE_ADMISSION_STAGE,
                NULL                                                                      ADMISSION_DATE,
                COURSE_ADMISSION_STAGE_CODE                                               COURSE_ADMISSION_STATUS_CODE,
                SSP_STG_CODE.CODE_DESCR                                                   COURSE_ADMISSION_STATUS,
                P.EFFCT_START_DT                                                          EFFCT_START_DT,
                --MAY NEED TO REVISIT JOIN
                CTE_3.ADMITTED_DATE                                                       COURSE_ADMISSION_ADMITTED_DATE,
                COALESCE(APPLICATION_PATH.APPLICATION_PATH, 'Unknown')                    APPLICATION_PATH,
                CTE_3.SSP_NO                                                              CS_SSP_NO,
                UN_FIRST_ENR.FIRST_ENROL_DATE                                             FIRST_UNIT_ENROLLED_DATE,
                'N'                                                                       IS_DELETED,
                CAT_TYPE.CRS_TYPE_CD                                                      COURSE_TYPE_CODE,
                CRS_TYPE_CODE.CODE_DESCR                                                  COURSE_TYPE_DESCRIPTION,
                FOE.PRIMARY_FIELD_OF_EDUCATION_CODE                                       PRIMARY_FIELD_OF_EDUCATION_CODE,
                FOE.PRIMARY_FIELD_OF_EDUCATION_DESC                                       PRIMARY_FIELD_OF_EDUCATION_DESC,
                FOE.SECONDARY_FIELD_OF_EDUCATION_CODE                                     SECONDARY_FIELD_OF_EDUCATION_CODE,
                FOE.SECONDARY_FIELD_OF_EDUCATION_DESC                                     SECONDARY_FIELD_OF_EDUCATION_DESC,
                CIRCOS_COURSE.CORRSPND_CD                                                 CRICOS_CODE,
                FOE_SIX.CODE                                                              FIELD_OF_EDUCATION_6_DIGIT_CODE,
                FOE_SIX.DESCRIPTION                                                       FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
                FOE_FOUR.CODE                                                             FIELD_OF_EDUCATION_4_DIGIT_CODE,
                FOE_FOUR.DESCRIPTION                                                      FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
                FOE_TWO.CODE                                                              FIELD_OF_EDUCATION_2_DIGIT_CODE,
                FOE_TWO.DESCRIPTION                                                       FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
                CTE_3.SUBMISSION_DT                                                       SUBMISSION_DT,
                CTE_3.SUBMISSION_METHOD_CD                                                SUBMISSION_METHOD_CD,
                SBM_CODE.CODE_DESCR                                                       SUBMISSION_METHOD,
                MD5
                    (
                            IFNULL(STUDENT_ID, '') || ',' ||
                            IFNULL(CS_SPK_NO, 0) || ',' ||
                            IFNULL(CS_SPK_VER_NO, 0) || ',' ||
                            IFNULL(CTE_3.APPN_NO, '') || ',' ||
                            IFNULL(APPLICATION_TYPE_CODE, '') || ',' ||
                            IFNULL(APPLICATION_TYPE, '') || ',' ||
                            IFNULL(ADMISSION_CENTRE_COURSE_CODE, '') || ',' ||
                            IFNULL(ADMISSION_CENTRE_APPLICANT_ID, '') || ',' ||
                            IFNULL(LIABILITY_CATEGORY_CODE, '') || ',' ||
                            IFNULL(LIABILITY_CATEGORY, '') || ',' ||
                            IFNULL(LOAD_CATEGORY_CODE, '') || ',' ||
                            IFNULL(LOAD_CATEGORY, '') || ',' ||
                            IFNULL(ATTENDANCE_MODE_CODE, '') || ',' ||
                            IFNULL(ATTENDANCE_MODE, '') || ',' ||
                            IFNULL(TO_CHAR(APPLICATION_DATE, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(APPLICATION_STAGE_CODE, '') || ',' ||
                            IFNULL(APPLICATION_STAGE, '') || ',' ||
                            IFNULL(APPLICATION_STATUS_CODE, '') || ',' ||
                            IFNULL(APPLICATION_STATUS, '') || ',' ||
                            IFNULL(OWNING_ORG_UNIT_CODE, '') || ',' ||
                            IFNULL(OWNING_ORG_UNIT_NAME, '') || ',' ||
                            IFNULL(APPLICATION_PREFERENCE_NUMBER, 0) || ',' ||
                            IFNULL(OFFER_PREFERENCE_NUMBER, 0) || ',' || -- Changed
                            IFNULL(OFFER_ROUND, '') || ',' ||
                            IFNULL(COURSE_CODE, '') || ',' ||
                            IFNULL(COURSE_FULL_TITLE, '') || ',' ||
                            IFNULL(HDR_COURSE, '') || ',' ||
                            IFNULL(COURSE_CATEGORY_TYPE_CODE, '') || ',' ||
                            IFNULL(COURSE_CATEGORY_TYPE, '') || ',' ||
                            IFNULL(OFFER_STATUS_CODE, '') || ',' ||
                            IFNULL(OFFER_STATUS, '') || ',' ||
                            IFNULL(TO_CHAR(OFFER_OFFERED_DATE, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(TO_CHAR(OFFER_ACCEPTED_DATE, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(TO_CHAR(OFFER_DEFERRED_DATE, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(TO_CHAR(OFFER_DECLINED_DATE, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(TO_CHAR(OFFER_ADMITTED_DATE), '') || ',' ||
                            IFNULL(TO_CHAR(OFFER_EXPIRED_DATE), '') || ',' ||
                            IFNULL(TO_CHAR(OFFER_LAPSED_DATE, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(TO_CHAR(OFFER_RESCINDED_DATE, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(TO_CHAR(OFFER_UNOFFERED_DATE), '') || ',' ||
                            IFNULL(OFFER_ACCEPTANCE, '') || ',' ||
                            IFNULL(TO_CHAR(OFFER_STATUS_EFFECTIVE_DATE), '') || ',' ||
                            IFNULL(COURSE_ADMISSION_STAGE_CODE, '') || ',' ||
                            IFNULL(COURSE_ADMISSION_STAGE, '') || ',' ||
                            IFNULL(TO_CHAR(COURSE_ADMISSION_ADMITTED_DATE, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(COURSE_ADMISSION_STATUS_CODE, '') || ',' ||
                            IFNULL(COURSE_ADMISSION_STATUS, '') || ',' ||
                            IFNULL(APPLICATION_PATH, '') || ',' ||
                            IFNULL(CS_SSP_NO, 0) || ',' ||
                            IFNULL(TO_CHAR(FIRST_UNIT_ENROLLED_DATE, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(IS_DELETED, '') || ',' ||
                            IFNULL(COURSE_TYPE_CODE, '') || ',' ||
                            IFNULL(COURSE_TYPE_DESCRIPTION, '') || ',' ||
                            IFNULL(PRIMARY_FIELD_OF_EDUCATION_CODE, '') || ',' ||
                            IFNULL(PRIMARY_FIELD_OF_EDUCATION_DESC, '') || ',' ||
                            IFNULL(SECONDARY_FIELD_OF_EDUCATION_CODE, '') || ',' ||
                            IFNULL(SECONDARY_FIELD_OF_EDUCATION_DESC, '') || ',' ||
                            IFNULL(CRICOS_CODE, '') || ',' ||
                            IFNULL(FIELD_OF_EDUCATION_6_DIGIT_CODE, '') || ',' ||
                            IFNULL(FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION, '') || ',' ||
                            IFNULL(FIELD_OF_EDUCATION_4_DIGIT_CODE, '') || ',' ||
                            IFNULL(FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION, '') || ',' ||
                            IFNULL(FIELD_OF_EDUCATION_2_DIGIT_CODE, '') || ',' ||
                            IFNULL(FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION, '') || ',' ||
                            IFNULL(CTE_3.SUBMISSION_METHOD_CD, '') || ',' ||
                            IFNULL(TO_CHAR(CTE_3.SUBMISSION_DT, 'YYYY-MM-DD'), '') || ',' ||
                            IFNULL(SUBMISSION_METHOD, ''))
                                                                                          HASH_MD5
         FROM SAT_APP_OFFER CTE_3
                  JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                       ON CS_SPK_DET.SPK_NO = CTE_3.SPK_NO
                           AND CS_SPK_DET.SPK_VER_NO = CTE_3.SPK_VER_NO
                           AND CS_SPK_DET.SPK_CAT_CD = 'CS'
                  LEFT JOIN ODS.AMIS.S1SSP_STU_SPK AS P
                            ON P.STU_ID = CTE_3.STU_ID
                                AND P.SSP_NO = CTE_3.SSP_NO
                                AND P.SPK_NO = CTE_3.SPK_NO
                                AND P.SPK_VER_NO = CTE_3.SPK_VER_NO
                  LEFT JOIN AVAIL_ORG as X
                            ON X.AVAIL_KEY_NO = CTE_3.AVAIL_KEY_NO
                                AND X.SPK_NO = CTE_3.SPK_NO
                                AND X.SPK_VER_NO = CTE_3.SPK_VER_NO
                                AND X.RN = 1
                  LEFT JOIN ODS.AMIS.S1CAT_TYPE CAT_TYPE
                            ON CAT_TYPE.SPK_CAT_TYPE_CD = CS_SPK_DET.SPK_CAT_TYPE_CD
                  LEFT JOIN ODS.AMIS.S1STC_CODE AS CD
                            ON CD.CODE_ID = LIABILITY_CATEGORY_CD
                                AND CODE_TYPE = 'LIAB_CAT_CD'
                  LEFT JOIN APPLICATION_PATH
                            ON APPLICATION_PATH.SSP_NO = CTE_3.SSP_NO
                                AND APPLICATION_PATH.STU_ID = CTE_3.STU_ID
                  LEFT JOIN ODS.AMIS.S1STC_CODE APPN_TYPE_CODE
                            ON APPN_TYPE_CODE.CODE_ID = CTE_3.SUBMISSION_METHOD_CD
                                AND APPN_TYPE_CODE.CODE_TYPE = 'APPN_TYPE_CD'
                  LEFT JOIN ODS.AMIS.S1STC_CODE LOAD_CAT_CODE
                            ON LOAD_CAT_CODE.CODE_ID = CTE_3.LOAD_CATEGORY_CD
                                AND LOAD_CAT_CODE.CODE_TYPE = 'LOAD_CAT_CD'
                  LEFT JOIN ODS.AMIS.S1STC_CODE ATTNDC_MODE_CODE
                            ON ATTNDC_MODE_CODE.CODE_ID = CTE_3.ATTENDANCE_MODE_CD
                                AND ATTNDC_MODE_CODE.CODE_TYPE = 'ATTNDC_MODE_CD'
                  LEFT JOIN ODS.AMIS.S1STC_CODE OFFER_OUTCOME
                            ON OFFER_OUTCOME.CODE_ID = CTE_3.OFFER_STATUS_CODE
                                AND OFFER_OUTCOME.CODE_TYPE = 'OFFER_OUTCOME_CD'
                  LEFT JOIN ODS.AMIS.S1STC_CODE SBM_CODE
                            ON SBM_CODE.CODE_ID = CTE_3.SUBMISSION_METHOD_CD
                                AND SBM_CODE.CODE_TYPE = 'SUBMISSION_METHOD_CD'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SSP_STG_CODE
                                  ON SSP_STG_CODE.CODE_ID = P.SSP_STG_CD
                                      AND SSP_STG_CODE.CODE_TYPE = 'SSP_STG_CD'
                  LEFT JOIN ORG_UNIT
                            ON ORG_UNIT.ORG_UNIT_CD = X.ORG_UNIT_CD
                                AND ORG_UNIT.RN = 1
                  LEFT JOIN CS_HIST
                            ON CS_HIST.SSP_NO = CTE_3.SSP_NO
                  LEFT JOIN UN_FIRST_ENR
                            ON UN_FIRST_ENR.CS_SSP = CTE_3.SSP_NO
                  LEFT JOIN ODS.AMIS.S1STC_CODE CRS_TYPE_CODE
                            ON CAT_TYPE.CRS_TYPE_CD = CRS_TYPE_CODE.CODE_ID
                                AND CRS_TYPE_CODE.CODE_TYPE = 'CRS_TYPE_CD'
                  LEFT OUTER JOIN FOE
                                  ON FOE.SPK_NO = CTE_3.SPK_NO
                                      AND FOE.SPK_VER_NO = CTE_3.SPK_VER_NO
                  LEFT JOIN CIRCOS_COURSE
                            ON CIRCOS_COURSE.SPK_NO = CS_SPK_DET.SPK_NO
                                AND CIRCOS_COURSE.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
                  LEFT JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_SIX
                            ON FOE_SIX.CODE = LPAD(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, 6, '0')
                  LEFT JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_FOUR
                            ON FOE_FOUR.CODE = SUBSTR(LPAD(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, 6, '0'), 1, 4)
                  LEFT JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_TWO
                            ON FOE_TWO.CODE = SUBSTR(LPAD(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, 6, '0'), 1, 2)
     )

SELECT DATA_VAULT.CORE.SEQ.nextval                                   as SAT_COURSE_APPLICATION_SUM_SK,
       HUB_COURSE_APPLICATION_KEY,
       'AMIS'                                                        as SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ                              as LOAD_DTS,
       CONCAT('SQL', CURRENT_TIMESTAMP::TIMESTAMP_NTZ)::varchar(100) as ETL_JOB_ID,
       HASH_MD5,
       STUDENT_ID,
       CS_SPK_NO,
       CS_SPK_VER_NO,
       APPN_NO,
       APPLICATION_TYPE_CODE,
       APPLICATION_TYPE,
       ADMISSION_CENTRE_COURSE_CODE,
       ADMISSION_CENTRE_APPLICANT_ID,
       LIABILITY_CATEGORY_CODE,
       LIABILITY_CATEGORY,
       LOAD_CATEGORY_CODE,
       LOAD_CATEGORY,
       ATTENDANCE_MODE_CODE,
       ATTENDANCE_MODE,
       APPLICATION_DATE,
       APPLICATION_STAGE_CODE,
       APPLICATION_STAGE,
       APPLICATION_STATUS_CODE,
       APPLICATION_STATUS,
       OWNING_ORG_UNIT_CODE,
       OWNING_ORG_UNIT_NAME,
       APPLICATION_PREFERENCE_NUMBER,
       OFFER_PREFERENCE_NUMBER,
       OFFER_ROUND,
       COURSE_CODE,
       COURSE_FULL_TITLE,
       HDR_COURSE,
       COURSE_CATEGORY_TYPE_CODE,
       COURSE_CATEGORY_TYPE,
       OFFER_STATUS_CODE,
       OFFER_STATUS,
       OFFER_OFFERED_DATE,
       OFFER_ACCEPTED_DATE,
       OFFER_DEFERRED_DATE,
       OFFER_DECLINED_DATE,
       OFFER_ADMITTED_DATE,
       OFFER_EXPIRED_DATE,
       OFFER_LAPSED_DATE,
       OFFER_RESCINDED_DATE,
       OFFER_UNOFFERED_DATE,
       OFFER_ACCEPTANCE,
       OFFER_STATUS_EFFECTIVE_DATE,
       COURSE_ADMISSION_STAGE_CODE,
       COURSE_ADMISSION_STAGE,
       COURSE_ADMISSION_ADMITTED_DATE,
       COURSE_ADMISSION_STATUS_CODE,
       COURSE_ADMISSION_STATUS,
       APPLICATION_PATH,
       CS_SSP_NO,
       FIRST_UNIT_ENROLLED_DATE,
       IS_DELETED,
       COURSE_TYPE_CODE,
       COURSE_TYPE_DESCRIPTION,
       PRIMARY_FIELD_OF_EDUCATION_CODE,
       PRIMARY_FIELD_OF_EDUCATION_DESC,
       SECONDARY_FIELD_OF_EDUCATION_CODE,
       SECONDARY_FIELD_OF_EDUCATION_DESC,
       CRICOS_CODE,
       FIELD_OF_EDUCATION_6_DIGIT_CODE,
       FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
       FIELD_OF_EDUCATION_4_DIGIT_CODE,
       FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
       FIELD_OF_EDUCATION_2_DIGIT_CODE,
       FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
       SUBMISSION_DT,
       SUBMISSION_METHOD_CD,
       SUBMISSION_METHOD
FROM SAT_COURSE_APPLICATION_SUM_INSERT SAT
WHERE NOT EXISTS(
        SELECT NULL
        FROM SAT_APPLICATION_SUM SM
        WHERE SM.EFFECTIVE_END_DTS IS NULL
          AND SM.HUB_COURSE_APPLICATION_KEY = SAT.HUB_COURSE_APPLICATION_KEY
          AND SM.HASH_MD5 = SAT.HASH_MD5
    );


