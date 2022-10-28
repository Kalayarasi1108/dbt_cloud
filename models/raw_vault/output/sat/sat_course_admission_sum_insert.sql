-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_COURSE_ADMISSION_SUM (SAT_COURSE_ADMISSION_SUM_SK,
                                                      HUB_COURSE_ADMISSION_KEY, SOURCE,
                                                      LOAD_DTS,
                                                      ETL_JOB_ID, HASH_MD5, STUDENT_ID, PARENT_SSP_NO,
                                                      ADMISSION_DATE,
                                                      ATTENDANCE_MODE, ATTENDANCE_TYPE,
                                                      EQUIVALENT_YEAR_LEVEL,
                                                      COURSE_LIABILITY_CATEGORY,
                                                      CURRENT_LIABILITY_CATEGORY,
                                                      COURSE_ANNUAL_CREDIT_POINTS,
                                                      COURSE_TOTAL_CREDIT_POINTS,
                                                      ADVANCED_STANDING_CREDIT_POINTS,
                                                      CREDITED_CREDIT_POINTS,
                                                      COMPLETED_CREDIT_POINTS, ENROLLED_CREDIT_POINTS,
                                                      COURSE_CAT_TYPE_CODE, COURSE_CAT_TYPE, COURSE_CODE,
                                                      COURSE_VERSION, COURSE_FULL_TITLE, CONSUMED_EFTSL,
                                                      COURSE_TOTAL_EFTSL, APPLICATION_PATH, SSP_STAGE,
                                                      SSP_STATUS,
                                                      CAMPUS, AVAILABILITY_YEAR, COURSE_SPK_NO,
                                                      AVAIL_KEY_NO, AVAIL_NO,
                                                      STUDY_MODE, STUDY_TYPE, STUDY_BASIS, HDR_COURSE,
                                                      COMMENCING_OR_CONTINUING, COURSE_CAT_TYPE_LEVEL,
                                                      UNIT_COMPLETE_ATTEMPT, UNIT_FAIL_NUMBER,
                                                      UNIT_PASS_NUMBER,
                                                      REPARENTED_COURSE, COURSE_COMPLETION_DATE,
                                                      CONFERRAL_DATE,
                                                      CURRENT_WAM, UNIT_ENROLLED, UNIT_ENROLLED_NUMBER,
                                                      GOV_REPORTED_FLAG, CURRENT_YEAR_ENROLMENT,
                                                      CROSS_INSTITUTION_COURSE,
                                                      COURSE_OWNING_ORG_UNIT_CODE,
                                                      COURSE_OWNING_ORG_UNIT_TYPE,
                                                      COURSE_OWNING_ORG_UNIT_NAME,
                                                      COURSE_EXPECTED_COMPLETION_DATE, COURSE_START_DATE,
                                                      COURSE_OF_STUDY_START_DATE,
                                                      COURSE_MUST_COMPLETE_DATE,
                                                      CONSOLIDATED, CURRENT_ATTENDANCE_MODE,
                                                      COURSE_TYPE_CODE, COURSE_TYPE_DESCRIPTION,
                                                      PRIMARY_FIELD_OF_EDUCATION_CODE,
                                                      PRIMARY_FIELD_OF_EDUCATION_DESC,
                                                      SECONDARY_FIELD_OF_EDUCATION_CODE,
                                                      SECONDARY_FIELD_OF_EDUCATION_DESC,
                                                      CRICOS_CODE, FIRST_UNIT_ENROLLED_DATE, HDR_PROGRAM,
                                                      FIELD_OF_EDUCATION_6_DIGIT_CODE,
                                                      FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
                                                      FIELD_OF_EDUCATION_4_DIGIT_CODE,
                                                      FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
                                                      FIELD_OF_EDUCATION_2_DIGIT_CODE,
                                                      FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
                                                      SPECIAL_COURSE_CODE,
                                                      STATUS_EFFECTIVE_DATE,
                                                      STAGE_CODE,
                                                      STATUS_CODE,
                                                      COURSE_END_DATE,
                                                      CURRENT_YEAR_EFTSL,
                                                      IS_DELETED,
                                                      ACADEMIC_STATUS,
                                                      ACADEMIC_STATUS_DESCRIPTION,
                                                      COURSE_OWNING_ORG_UNIT_CODE_BY_AVAILABILITY,
                                                      COURSE_OWNING_ORG_UNIT_TYPE_BY_AVAILABILITY,
                                                      COURSE_OWNING_ORG_UNIT_NAME_BY_AVAILABILITY)

WITH CURRENT_LIABILITY AS (
    SELECT SSP_NO, COALESCE(A.UN_LIAB_CAT_CD, A.CS_LIAB_CAT_CD) CURR_LIAB_CAT_CD
    FROM (
             SELECT CS_SSP.SSP_NO,
                    CS_SSP.LIAB_CAT_CD       CS_LIAB_CAT_CD,
                    --CS_SSP.SSP_STTS_CD CS_SSP_STTS_CD,
                    UN_SSP.LIAB_CAT_CD       UN_LIAB_CAT_CD,
                    --UN_SSP.SSP_STTS_CD UN_SSP_STTS_CD,
                    --SSP_STTS_HIST.SSP_STTS_CD UN_SSP_HIST_STTS_CD,
                    SSP_STTS_HIST.EFFCT_START_DT,
                    ROW_NUMBER() OVER (PARTITION BY CS_SSP.SSP_NO ORDER BY
                        UN_SSP.CENSUS_DT DESC NULLS LAST,
                        SSP_STTS_HIST.EFFCT_START_DT DESC NULLS LAST,
                        SSP_STTS_HIST.SSP_STTS_NO DESC NULLS LAST,
                        UN_SSP.LIAB_CAT_CD ) RN
             FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                      LEFT OUTER JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                                      ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO AND
                                         UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
                      LEFT OUTER JOIN ODS.AMIS.S1SSP_STTS_HIST SSP_STTS_HIST
                                      ON SSP_STTS_HIST.SSP_NO = UN_SSP.SSP_NO AND
                                         SSP_STTS_HIST.SSP_STTS_CD IN ('ENR', 'UX', 'PASS', 'FAIL')
             WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
         ) A
    WHERE A.RN = 1
)
   , ADVANCED_STANDING AS (
    SELECT SSP_ADV.PARENT_SSP_NO, SUM(VALUE_REMAINING) ADV_CP
    FROM ODS.AMIS.S1SSP_STU_SPK SSP_ADV
    WHERE SSP_ADV.SSP_STTS_CD = 'ADV'
    GROUP BY SSP_ADV.PARENT_SSP_NO
)
   , CREDIT_POINTS AS (
    SELECT A.PARENT_SSP_NO,
           SUM(IFF(A.SSP_STTS_CD IN ('CR', 'DES', 'EX'), UN_CP_VAL, NULL))         CREDITED_CREDIT_POINTS,
           SUM(IFF(A.SSP_STTS_CD IN ('PASS', 'CR', 'DES', 'EX'), UN_CP_VAL, NULL)) COMPLETED_CREDIT_POINTS,
           SUM(IFF(A.SSP_STTS_CD IN ('ENR', 'UX', 'LOA'), UN_CP_VAL, NULL))        ENROLLED_CREDIT_POINTS
    FROM (SELECT UN_SSP.PARENT_SSP_NO,
                 UN_SSP.SSP_NO,
                 UN_SSP.STU_ID,
                 UN_SPK_DET.SPK_CD                      UN_SPK_CD,
                 UN_SPK_STUDY_MEASURE.STUDY_MEASURE_VAL UN_CP_VAL,
                 UN_SSP.AVAIL_YR,
                 UN_SSP.SSP_STTS_CD,
                 UN_SSP.EFFCT_START_DT,
                 UN_SSP.NOT_ON_PLAN_FG
          FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
                   JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
                        ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO AND
                           UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO AND
                           UN_SPK_DET.SPK_CAT_CD = 'UN'
                   JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                        ON CS_SPK_DET.SPK_NO = UN_SSP.PARENT_SPK_NO AND
                           CS_SPK_DET.SPK_VER_NO = UN_SSP.PARENT_SPK_VER_NO AND
                           CS_SPK_DET.SPK_CAT_CD = 'CS'
                   JOIN ODS.AMIS.S1SPK_STUDY_MEASURE UN_SPK_STUDY_MEASURE
                        ON UN_SPK_STUDY_MEASURE.SPK_NO = UN_SSP.SPK_NO AND
                           UN_SPK_STUDY_MEASURE.SPK_VER_NO = UN_SSP.SPK_VER_NO AND
                           UN_SPK_STUDY_MEASURE.STUDY_MEASURE_CD =
                           CS_SPK_DET.DFLT_STUDY_MEASURE_CD
          WHERE UN_SSP.SSP_STTS_CD IN ('PASS',
                                       'CR',
                                       'DES',
                                       'EX',
                                       'ENR',
                                       'UX',
                                       'LOA')
            AND UN_SSP.NOT_ON_PLAN_FG = 'N'
         ) A
    GROUP BY A.PARENT_SSP_NO
)
   , HDR_CONSUMED_EFTSL AS (
    SELECT CS_SSP.SSP_NO,
           SUM(SSP_EP_DTL.GROSS_NORM_EFTSU + SSP_EP_DTL.GROSS_FEC_EFTSU) CONSUMED_EFTSL
    FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
             JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                  ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO AND
                     UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
             JOIN ODS.AMIS.S1SSP_EP_DTL SSP_EP_DTL
                  ON SSP_EP_DTL.SSP_NO = UN_SSP.SSP_NO
                      AND SSP_EP_DTL.EP_YEAR <= UN_SSP.CUR_EP_YEAR
                      AND YEAR(SSP_EP_DTL.CTL_CENSUS_DT) > 1900
                      AND SSP_EP_DTL.EP_CYR_RD_FG = 'Y'
    WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
      AND UN_SSP.STUDY_BASIS_CD = '$TIME'
    GROUP BY CS_SSP.SSP_NO
)
   , SPK_OWN_ORG_UNIT AS (
    SELECT SPK_NO, SPK_VER_NO, MAX(ORG_UNIT_CD) ORG_UNIT_CD
    FROM ODS.AMIS.S1SPK_ORG_UNIT
    WHERE RESP_CAT_CD = 'O'
    GROUP BY SPK_NO, SPK_VER_NO
)
   , OWN_ORG_UNIT AS (
    SELECT A.ORG_UNIT_CD,
           A.ORG_UNIT_SHORT_NM,
           A.ORG_UNIT_TYPE_CD,
           A.ORG_UNIT_TYPE,
           A.ORG_UNIT_NM
    FROM (
             SELECT ORG_UNIT.ORG_UNIT_CD,
                    ORG_UNIT.EFFCT_DT,
                    ORG_UNIT.EXPIRY_DT,
                    ORG_UNIT.ORG_UNIT_SHORT_NM,
                    ORG_UNIT.ORG_UNIT_TYPE_CD,
                    ORG_UNIT.ORG_UNIT_NM,
                    ORG_TYPE_CODE.CODE_DESCR                                                         ORG_UNIT_TYPE,
                    ROW_NUMBER()
                            OVER (PARTITION BY ORG_UNIT.ORG_UNIT_CD ORDER BY ORG_UNIT.EFFCT_DT DESC) RN
             FROM ODS.AMIS.S1ORG_UNIT ORG_UNIT
                      LEFT OUTER JOIN ODS.AMIS.S1STC_CODE ORG_TYPE_CODE
                                      ON ORG_TYPE_CODE.CODE_ID = ORG_UNIT.ORG_UNIT_TYPE_CD
                                          AND ORG_TYPE_CODE.CODE_TYPE = 'ORG_TYPE_CD'
         ) A
    WHERE A.RN = 1
)
   , AVAIL_ORG AS (
    SELECT SPK_NO,
           SPK_VER_NO,
           AVAIL_KEY_NO,
           ORG_UNIT_CD,
           ROW_NUMBER() OVER (PARTITION BY SPK_NO, SPK_VER_NO, AVAIL_KEY_NO ORDER BY AVAIL_KEY_NO DESC) AS RN
    FROM ODS.AMIS.S1SPK_AVAIL_ORG
    WHERE RESP_CAT_CD = 'O'
)
   , NEW_APPLICATION AS (
    SELECT S.STU_ID,
           S.SSP_NO,
           S.SPK_NO,
           S.SPK_VER_NO,
           CONCAT(L.APPLICATION_ID, '_', L.APPLICATION_LINE_ID) AS APPN_NO,
           AVAIL_ORG.ORG_UNIT_CD                                   OWNING_ORG_UNIT_CD,
           A.SUBMISSION_METHOD_CD

    FROM ODS.AMIS.S1APP_APPLICATION AS A
             INNER JOIN ODS.AMIS.S1APP_APPLICATION_LINE AS L ON A.APPLICATION_ID = L.APPLICATION_ID
             INNER JOIN ODS.AMIS.S1APP_STUDY AS S
                        ON L.APPLICATION_ID = S.APPLICATION_ID AND S.APPLICATION_LINE_ID = L.APPLICATION_LINE_ID
             INNER JOIN ODS.AMIS.S1SPK_AVAIL_DET AVAIL_DET
                        ON AVAIL_DET.AVAIL_KEY_NO = S.AVAIL_KEY_NO
             LEFT OUTER JOIN AVAIL_ORG
                             ON AVAIL_ORG.SPK_NO = S.SPK_NO AND
                                AVAIL_ORG.SPK_VER_NO = S.SPK_VER_NO
                                 AND AVAIL_DET.AVAIL_KEY_NO = AVAIL_ORG.AVAIL_KEY_NO
                                 AND AVAIL_ORG.RN = 1
)
   , APPLICATION_PATH AS
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
                   ELSE DECODE(SUBMISSION_METHOD_CD, 'SP', 'HDRO', 'OS', 'Direct', 'D', 'Direct', 'RPA', 'StudyLink',
                               'AP',
                               'StudyLink', 'SL', 'StudyLink', 'SI', 'StudyLink', 'A', 'UAC', 'C', 'UAC', 'DU', 'UAC',
                               'P',
                               'UAC', 'PO', 'UAC', 'U', 'UAC', 'AASN', 'Other', 'ADJ', 'Other', 'UP', 'Other', 'VP',
                               'Other',
                               'Other')
                   END APPLICATION_PATH
        FROM NEW_APPLICATION APPLICATION
                 JOIN ODS.AMIS.S1SSP_STU_SPK CS_SSP
                      ON APPLICATION.STU_ID = CS_SSP.STU_ID
                          AND APPLICATION.SSP_NO = CS_SSP.SSP_NO
                 JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                      ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                          AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
        WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
        UNION ALL
        SELECT CS_SSP.SSP_NO,
               CS_SSP.STU_ID,
               CASE
                   WHEN CS_SSP.SPRD_CD LIKE 'OLA%' OR
                        CS_SSP.SPRD_CD LIKE 'OUA%' THEN 'OUA'
                   WHEN STU_APPLICATION.OWNING_ORG_UNIT_CD = '8245' THEN 'StudyLink'
                   WHEN SPK_CAT_TYPE_CD IN ('100', '110', '122', '125', '130', '132', '170', '178') AND
                        APPN_TYPE_CD NOT IN ('I', 'IA', 'SL') THEN 'HDRO'
                   WHEN APPN_TYPE_CD IS NULL THEN 'Not Entered'
                   ELSE DECODE(STU_APPLICATION.APPN_TYPE_CD, 'HD', 'HDRO', 'SP', 'HDRO', 'D', 'Direct', 'OS', 'Direct',
                               'I',
                               'StudyLink', 'IA', 'StudyLink', 'SL', 'StudyLink', 'A', 'UAC', 'C', 'UAC', 'DU', 'UAC',
                               'P',
                               'UAC', 'PO', 'UAC', 'U', 'UAC', 'OT', 'Other', 'VP', 'Other', 'NA', 'Other', 'Other')
                   END APPLICATION_PATH
        FROM ODS.AMIS.S1STU_APPLICATION STU_APPLICATION
                 JOIN ODS.AMIS.S1SSP_STU_SPK CS_SSP
                      ON STU_APPLICATION.STU_ID = CS_SSP.STU_ID
                          AND STU_APPLICATION.SPK_NO = CS_SSP.APPN_SPK_NO
                          AND STU_APPLICATION.SPK_VER_NO = CS_SSP.APPN_VER_NO
                          AND STU_APPLICATION.APPN_NO = CS_SSP.APPN_NO
                 JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                      ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                          AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
        WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
          AND APPLICATION_ID = 0
          AND APPLICATION_LINE_ID = 0
    )
   , CURR_YEAR_ENR_SSP AS
    (
        -- COURSE WORK
        SELECT CS_SSP.SSP_NO
        from ODS.AMIS.S1SSP_STU_SPK CS_SSP
                 JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                      ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO
        WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
          AND (
                (TO_CHAR(UN_SSP.AVAIL_YR) = TO_CHAR(CURRENT_TIMESTAMP::TIMESTAMP_NTZ
                    , 'YYYY')
                    AND
                 UN_SSP.SSP_STTS_CD IN ('ENR', 'UX'))
                OR
                (TO_CHAR(UN_SSP.CENSUS_DT
                     , 'YYYY') = TO_CHAR(CURRENT_TIMESTAMP::TIMESTAMP_NTZ
                     , 'YYYY'))
            )
          AND UN_SSP.STUDY_BASIS_CD != '$TIME'
        UNION
        -- TIME BASED
        select CS_SSP.SSP_NO
        from ODS.AMIS.S1SSP_STU_SPK CS_SSP
                 JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                      ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO AND UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
                 JOIN ODS.AMIS.S1SSP_EP_DTL SSP_EP_DTL
                      ON SSP_EP_DTL.SSP_NO = UN_SSP.SSP_NO AND
                         TO_CHAR(SSP_EP_DTL.EP_YEAR) = TO_CHAR(CURRENT_TIMESTAMP::TIMESTAMP_NTZ, 'YYYY')
        WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
          AND UN_SSP.STUDY_BASIS_CD = '$TIME'
          AND UN_SSP.CUR_EP_YEAR >= SSP_EP_DTL.EP_YEAR
          AND YEAR(SSP_EP_DTL.CTL_CENSUS_DT)
            > 1900
          AND SSP_EP_DTL.EP_CYR_RD_FG = 'Y'
          AND (
                ((SSP_EP_DTL.GROSS_NORM_EFTSU + SSP_EP_DTL.GROSS_FEC_EFTSU)
                     > 0
                    OR
                 YEAR(SSP_EP_DTL.CTL_CENSUS_DT) = YEAR(CURRENT_DATE)
                    )
                AND
                (TO_CHAR(UN_SSP.CENSUS_DT
                     , 'YYYY') = TO_CHAR(CURRENT_TIMESTAMP::TIMESTAMP_NTZ
                     , 'YYYY'))
            )
    )
   , UNIT_PASS_FAIL AS (
    SELECT CS_SSP.SSP_NO,
           SUM(IFF(UN_SSP_STTS_HIST.SSP_STTS_CD = 'FAIL', 1, 0))            FAIL_NUMBER,
           SUM(IFF(UN_SSP_STTS_HIST.SSP_STTS_CD = 'PASS', 1, 0))            PASS_NUMBER,
           SUM(IFF(UN_SSP_STTS_HIST.SSP_STTS_CD IN ('FAIL', 'PASS'), 1, 0)) FAIL_PASS_NUMBER
    FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
             JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                  ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO AND
                     UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
             JOIN ODS.AMIS.S1SSP_STTS_HIST UN_SSP_STTS_HIST
                  ON UN_SSP_STTS_HIST.SSP_NO = UN_SSP.SSP_NO
    WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
    GROUP BY CS_SSP.SSP_NO
)
   , CONFERRAL_DATE AS (
    SELECT A.SSP_NO, A.CONFERRAL_DT
    FROM (
             SELECT CS_SSP.SSP_NO,
                    AWD_DET.CONFERRAL_DT,
                    ROW_NUMBER()
                            OVER (PARTITION BY CS_SSP.SSP_NO ORDER BY AWD_DET.AWD_NO DESC) RN
             FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                      JOIN ODS.AMIS.S1AWD_DET AWD_DET
                           ON AWD_DET.SSP_NO = CS_SSP.SSP_NO
                               AND TO_CHAR(AWD_DET.CONFERRAL_DT, 'YYYY') != '1900'
             WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
               AND CS_SSP.SSP_STTS_CD = 'PASS'
         ) A
    WHERE RN = 1
)
   , UN_ENROLLED AS (
    SELECT CS_SSP.SSP_NO,
           COUNT(*) ENR_NO
    FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
             JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                  ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO AND
                     UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
                      AND UN_SSP.SSP_STTS_CD IN ('ENR', 'UX')
    WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
    GROUP BY CS_SSP.SSP_NO
)
   , SSP_DATE AS (
    SELECT A.SSP_NO,
           MAX(IFF(A.SSP_DT_TYPE_CD = 'MCOM', A.EXPECTED_DATE, NULL))  MUST_COMPLETE_BY,
           MAX(IFF(A.SSP_DT_TYPE_CD = 'MINCP', A.EXPECTED_DATE, NULL)) MINIMUM_TIME_TO_COMPL_BY,
           MAX(IFF(A.SSP_DT_TYPE_CD = 'COS', A.EXPECTED_DATE, NULL))   COURSE_OF_STUDY_START,
           MAX(IFF(A.SSP_DT_TYPE_CD = 'CAND', A.EXPECTED_DATE, NULL))  MQ_CANDIDACY_DUE_BY,
           MAX(IFF(A.SSP_DT_TYPE_CD = 'STRT', A.EXPECTED_DATE, NULL))  START_DATE,
           MAX(IFF(A.SSP_DT_TYPE_CD = '$EXPC', A.EXPECTED_DATE, NULL)) EXPECTED_COMPLETION,
           MAX(IFF(A.SSP_DT_TYPE_CD = '$PSAC', A.EXPECTED_DATE, NULL)) PARENT_ACTIVITY_START_DATE
    FROM (
             SELECT CS_SSP.SSP_NO,
                    CS_SSP_DATE.SSP_DT_TYPE_CD,
                    CS_SSP_DATE.DATE_NO,
                    CS_SSP_DATE.EXPECTED_DATE
             FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                      JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                           ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                               AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
                      LEFT OUTER JOIN ODS.AMIS.S1SSP_DATE CS_SSP_DATE
                                      ON CS_SSP_DATE.SSP_NO = CS_SSP.SSP_NO AND
                                         CS_SSP_DATE.SSP_DT_TYPE_CD IS NOT NULL
             WHERE CS_SSP.SSP_NO = PARENT_SSP_NO
         ) A
    GROUP BY A.SSP_NO
)
   , CURRENT_WAN AS (
    SELECT A.CS_SSP_NO, SUM(A.MARK * A.STUDY_MEASURE_VAL) / SUM(A.STUDY_MEASURE_VAL) WAM
    FROM (SELECT CS_SSP.SSP_NO CS_SSP_NO,
                 UN_SSP.MARK,
                 UN_SSP.VERIFIED_FG,
                 UN_SSP.RATIFIED_FG,
                 UN_SSP.SSP_STTS_CD,
                 UN_SPK_STUDY_MEASURE.STUDY_MEASURE_VAL
          FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                   JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                        ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO AND
                           UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
                            AND UN_SSP.CERTIFIED_FG = 'Y'
                            AND UN_SSP.SSP_STTS_CD NOT IN ('CR', 'DES', 'EX')
                   JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                        ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                            AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
                   JOIN ODS.AMIS.S1SPK_STUDY_MEASURE UN_SPK_STUDY_MEASURE
                        ON UN_SPK_STUDY_MEASURE.SPK_NO = UN_SSP.SPK_NO
                            AND UN_SPK_STUDY_MEASURE.SPK_VER_NO = UN_SSP.SPK_VER_NO
                            AND UN_SPK_STUDY_MEASURE.STUDY_MEASURE_CD =
                                CS_SPK_DET.DFLT_STUDY_MEASURE_CD
                   JOIN ODS.AMIS.S1RSL_DET RSL_DET
                        ON RSL_DET.RSLT_TYPE_CD = UN_SSP.RSLT_TYPE_CD
                            AND RSL_DET.GRADE_CD = UN_SSP.GRADE_CD
                            AND UN_SSP.RSLT_EFFCT_DT >= RSL_DET.EFFCT_DT
                            AND UN_SSP.RSLT_EFFCT_DT <=
                                IFF(YEAR(RSL_DET.EXPIRY_DT) = 1900, TO_DATE('2050', 'YYYY'),
                                    RSL_DET.EXPIRY_DT)
                            AND
                           (RSL_DET.MARKS_CD = '$Y' OR (RSL_DET.MARKS_CD = '$M' AND UN_SSP.MARK >= 0))
                            AND UN_SSP.PASS_FAIL_CD NOT IN ('FN', 'PN', 'ON')
          WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
            AND UN_SPK_STUDY_MEASURE.STUDY_MEASURE_VAL > 0
         ) A
    GROUP BY A.CS_SSP_NO
)
   , UNIT_ATTENDANCE AS (
    -- COURSE WORK
    SELECT CS_SSP.SSP_NO, UN_SSP.ATTNDC_MODE_CD
    from ODS.AMIS.S1SSP_STU_SPK CS_SSP
             JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                  ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO
    WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
      AND (
            (TO_CHAR(UN_SSP.AVAIL_YR) =
             TO_CHAR(CURRENT_TIMESTAMP::TIMESTAMP_NTZ
                 , 'YYYY')
                AND
             UN_SSP.SSP_STTS_CD IN ('ENR', 'UX'))
            OR
            (TO_CHAR(UN_SSP.CENSUS_DT
                 , 'YYYY') =
             TO_CHAR(CURRENT_TIMESTAMP::TIMESTAMP_NTZ
                 , 'YYYY'))
        )
      AND UN_SSP.STUDY_BASIS_CD != '$TIME'
    UNION
-- TIME BASED
    select CS_SSP.SSP_NO, UN_SSP.ATTNDC_MODE_CD
    from ODS.AMIS.S1SSP_STU_SPK CS_SSP
             JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                  ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO AND
                     UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
             JOIN ODS.AMIS.S1SSP_EP_DTL SSP_EP_DTL
                  ON SSP_EP_DTL.SSP_NO = UN_SSP.SSP_NO AND
                     TO_CHAR(SSP_EP_DTL.EP_YEAR) =
                     TO_CHAR(CURRENT_TIMESTAMP::TIMESTAMP_NTZ, 'YYYY')
                      AND SSP_EP_DTL.EP_CYR_RD_FG = 'Y'
    WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
      AND UN_SSP.STUDY_BASIS_CD = '$TIME'
      AND UN_SSP.CUR_EP_YEAR >= SSP_EP_DTL.EP_YEAR
      AND (
            ((SSP_EP_DTL.GROSS_NORM_EFTSU + SSP_EP_DTL.GROSS_FEC_EFTSU)
                 > 0
                OR
             YEAR(SSP_EP_DTL.CTL_CENSUS_DT) = YEAR(CURRENT_DATE)
                )
            AND
            (TO_CHAR(UN_SSP.CENSUS_DT
                 , 'YYYY') =
             TO_CHAR(CURRENT_TIMESTAMP::TIMESTAMP_NTZ
                 , 'YYYY'))
        )
)
   , CALC_COURSE_ATTENDANCE AS (
    SELECT UNIT_ATTENDANCE.SSP_NO,
           MIN(UNIT_ATTENDANCE.ATTNDC_MODE_CD) MIN_ATTNDC_MODE_CD,
           MAX(UNIT_ATTENDANCE.ATTNDC_MODE_CD) MAX_ATTNDC_MODE_CD
    FROM UNIT_ATTENDANCE
    GROUP BY UNIT_ATTENDANCE.SSP_NO
)
   , FOE AS (
    SELECT A.SPK_NO,
           A.SPK_VER_NO,
           MAX(IFF(A.PRIM_SECONDARY_CD = 'P', A.FOE_TRLN_CD, NULL)) PRIMARY_FIELD_OF_EDUCATION_CODE,
           MAX(IFF(A.PRIM_SECONDARY_CD = 'P', A.FOE_DESC, NULL))    PRIMARY_FIELD_OF_EDUCATION_DESC,
           MAX(IFF(A.PRIM_SECONDARY_CD = 'S', A.FOE_TRLN_CD, NULL)) SECONDARY_FIELD_OF_EDUCATION_CODE,
           MAX(IFF(A.PRIM_SECONDARY_CD = 'S', A.FOE_DESC, NULL))    SECONDARY_FIELD_OF_EDUCATION_DESC
    FROM (SELECT CS_SPK_DET.SPK_NO,
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
          WHERE CS_SPK_DET.SPK_CAT_CD = 'CS') A
    GROUP BY A.SPK_NO, A.SPK_VER_NO
)
   , CIRCOS_COURSE AS (
    SELECT CS_SPK_DET.SPK_NO,
           CS_SPK_DET.SPK_VER_NO,
           MAX(CS_SPK_CORRSPND_CD.CORRSPND_CD) CORRSPND_CD
    FROM ODS.AMIS.S1SPK_CORRSPND_CD CS_SPK_CORRSPND_CD
             JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                  ON CS_SPK_DET.SPK_NO = CS_SPK_CORRSPND_CD.SPK_NO
                      AND CS_SPK_DET.SPK_VER_NO = CS_SPK_CORRSPND_CD.SPK_VER_NO
                      AND CS_SPK_DET.SPK_CAT_CD = 'CS'
    WHERE 1 = 1
      AND CORRSPND_CD_TYPE = 'CRCOS'
      AND CORRSPND_CD != 'DISTANCE'
    GROUP BY CS_SPK_DET.SPK_NO, CS_SPK_DET.SPK_VER_NO
)
   , UN_FIRST_ENR AS (
    SELECT UN_SSP.PARENT_SSP_NO CS_SSP, MIN(HIST.EFFCT_START_DT) FIRST_ENROL_DATE
    FROM ODS.AMIS.S1SSP_STTS_HIST HIST
             JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                  ON UN_SSP.SSP_NO = HIST.SSP_NO
                      AND TO_CHAR(UN_SSP.CENSUS_DT, 'YYYY') > '1900'
    WHERE HIST.SSP_STTS_CD = 'ENR'
    GROUP BY UN_SSP.PARENT_SSP_NO
)
   , TIME_BASED_UNIT AS (
    SELECT A.PARENT_SSP_NO
    FROM ODS.AMIS.S1SSP_STU_SPK A
    WHERE A.SSP_STTS_CD IN ('ENR', 'LOA', 'UX', 'PASS', 'WD')
      AND A.STUDY_BASIS_CD = '$TIME'
    GROUP BY A.PARENT_SSP_NO
)
   , UNIT_EFTSL AS (
    SELECT UN_SSP.SSP_NO,
           UN_SSP.PARENT_SSP_NO,
           CASE
               -- WHEN CONSUMED_EFTSL.CONSUMED_EFTSL IS NOT NULL THEN CONSUMED_EFTSL.CONSUMED_EFTSL
               WHEN CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL IS NOT NULL AND
                    CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL > 0
                   THEN UN_SPK_STUDY_MEASURE.STUDY_MEASURE_VAL /
                        CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL
               WHEN CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL IS NULL AND
                    CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL <= 0
                   THEN UN_SPK_STUDY_MEASURE.STUDY_MEASURE_VAL /
                        CSP_STUDY_MEASURE_DET.ANNUAL_LOAD_VAL
               END CONSUMED_EFTSL
    FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
             JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
                  ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                      AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                      AND UN_SPK_DET.SPK_CAT_CD = 'UN'
             JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                  ON CS_SPK_DET.SPK_NO = UN_SSP.PARENT_SPK_NO
                      AND CS_SPK_DET.SPK_VER_NO = UN_SSP.PARENT_SPK_VER_NO
                      AND CS_SPK_DET.SPK_CAT_CD = 'CS'
             LEFT OUTER JOIN ODS.AMIS.S1SPK_STUDY_MEASURE UN_SPK_STUDY_MEASURE
                             ON UN_SPK_STUDY_MEASURE.SPK_NO = UN_SSP.SPK_NO
                                 AND
                                UN_SPK_STUDY_MEASURE.SPK_VER_NO = UN_SSP.SPK_VER_NO
                                 AND UN_SPK_STUDY_MEASURE.STUDY_MEASURE_CD =
                                     CS_SPK_DET.DFLT_STUDY_MEASURE_CD
             LEFT OUTER JOIN ODS.AMIS.S1SPK_STUDY_MEASURE CS_SPK_STUDY_MEASURE
                             ON CS_SPK_STUDY_MEASURE.SPK_NO = CS_SPK_DET.SPK_NO
                                 AND
                                CS_SPK_STUDY_MEASURE.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
                                 AND CS_SPK_STUDY_MEASURE.STUDY_MEASURE_CD =
                                     CS_SPK_DET.DFLT_STUDY_MEASURE_CD
             LEFT OUTER JOIN ODS.AMIS.S1CSP_STUDY_MEASURE_DET CSP_STUDY_MEASURE_DET
                             ON CSP_STUDY_MEASURE_DET.STUDY_MEASURE_CD =
                                CASE
                                    WHEN TRIM(CS_SPK_DET.DFLT_STUDY_MEASURE_CD) != '' AND
                                         CS_SPK_DET.DFLT_STUDY_MEASURE_CD IS NOT NULL
                                        THEN CS_SPK_DET.DFLT_STUDY_MEASURE_CD
                                    WHEN LENGTH(CS_SPK_DET.SPK_CD) < 8 THEN '$CP'
                                    ELSE 'CP2'
                                    END
    WHERE UN_SSP.STUDY_BASIS_CD = '$CRDT'
      AND YEAR(UN_SSP.CENSUS_DT) = YEAR(CURRENT_DATE)
    UNION ALL
    SELECT UN_SSP.SSP_NO,
           UN_SSP.PARENT_SSP_NO,
           SSP_EP_DTL.GROSS_NORM_EFTSU + SSP_EP_DTL.GROSS_FEC_EFTSU CONSUMED_EFTSL
    FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
             JOIN ODS.AMIS.S1SSP_EP_DTL SSP_EP_DTL
                  ON SSP_EP_DTL.SSP_NO = UN_SSP.SSP_NO
                      AND SSP_EP_DTL.EP_YEAR <= UN_SSP.CUR_EP_YEAR
         -- AND SSP_EP_DTL.EP_NO <= UN_SSP.CUR_EP_NO
    WHERE UN_SSP.SSP_NO != UN_SSP.PARENT_SSP_NO
      AND YEAR(SSP_EP_DTL.CTL_CENSUS_DT) = YEAR(CURRENT_DATE)
      AND SSP_EP_DTL.EP_YEAR = YEAR(CURRENT_DATE)
      AND SSP_EP_DTL.EP_CYR_RD_FG = 'Y'
      AND UN_SSP.STUDY_BASIS_CD = '$TIME'
)
   , COURSE_EFTSL AS (
    SELECT UNIT_EFTSL.PARENT_SSP_NO, SUM(CONSUMED_EFTSL) COURSE_EFTSL
    FROM UNIT_EFTSL
    GROUP BY UNIT_EFTSL.PARENT_SSP_NO
)
   , SAT_ADMISSION_SUM AS (
    SELECT MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
               IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
               IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
               IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
               IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
               IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
               IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
               IFNULL(CS_SSP.SSP_NO, 0)
               )                                                                                  HUB_COURSE_ADMISSION_KEY,
           CS_SSP.STU_ID                                                                          STUDENT_ID,
           CS_SSP.PARENT_SSP_NO                                                                   PARENT_SSP_NO,
           CS_SSP.SSP_NO                                                                          COURSE_SSP_NO,
           COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE, START_DATE.EXPECTED_DATE)              ADMISSION_DATE,
           ATTENDANCE_MODE_CODE.CODE_DESCR                                                        ATTENDANCE_MODE,
           CASE
               WHEN CURRENT_DATE < TO_DATE(YEAR(CURRENT_DATE)::STRING || '0701', 'YYYYMMDD')
                   THEN IFF(COURSE_EFTSL.COURSE_EFTSL >= 0.3749, 'Full Time', 'Part Time')
               WHEN CURRENT_DATE >= TO_DATE(YEAR(CURRENT_DATE)::STRING || '0701', 'YYYYMMDD')
                   THEN IFF(COURSE_EFTSL.COURSE_EFTSL >= 0.7490, 'Full Time', 'Part Time')
               ELSE 'Not entered'
               END                                                                                ATTENDANCE_TYPE,
           NULL                                                                                   EQUIVALENT_YEAR_LEVEL,
           COURSE_LIABILITY_CATEGORY_CODE.CODE_DESCR                                              COURSE_LIABILITY_CATEGORY,
           CURRENT_LIABILITY_CATEGORY_CODE.CODE_DESCR                                             CURRENT_LIABILITY_CATEGORY,
           IFF(CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL = 0, CSP_STUDY_MEASURE_DET.ANNUAL_LOAD_VAL,
               CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL)                                              COURSE_ANNUAL_CREDIT_POINTS,
           CS_SPK_STUDY_MEASURE.STUDY_MEASURE_VAL                                                 COURSE_TOTAL_CREDIT_POINTS,
           ADVANCED_STANDING.ADV_CP                                                               ADVANCED_STANDING_CREDIT_POINTS,
           CREDIT_POINTS.CREDITED_CREDIT_POINTS                                                   CREDITED_CREDIT_POINTS,
           CREDIT_POINTS.COMPLETED_CREDIT_POINTS                                                  COMPLETED_CREDIT_POINTS,
           CREDIT_POINTS.ENROLLED_CREDIT_POINTS                                                   ENROLLED_CREDIT_POINTS,
           CS_SPK_DET.SPK_CAT_TYPE_CD                                                             COURSE_CAT_TYPE_CODE,
           CAT_TYPE.SPK_CAT_TYPE_DESC                                                             COURSE_CAT_TYPE,
           CS_SPK_DET.SPK_CD                                                                      COURSE_CODE,
           CS_SPK_DET.SPK_VER_NO                                                                  COURSE_VERSION,
           CS_SPK_DET.SPK_FULL_TITLE                                                              COURSE_FULL_TITLE,
           HDR_CONSUMED_EFTSL.CONSUMED_EFTSL                                                      CONSUMED_EFTSL,
           CS_SPK_DET.EXP_EFTSU_TO_COMP                                                           COURSE_TOTAL_EFTSL,
           COALESCE(APPLICATION_PATH.APPLICATION_PATH, 'Unknown')                                 APPLICATION_PATH,
           APPLICATION_PATH.SSP_NO                                                                APP_SSP_NO,
           APPLICATION_PATH.STU_ID                                                                APP_STU_ID,
           SSP_STG_CODE.CODE_DESCR                                                                SSP_STAGE,
           SSP_STATUS_CODE.CODE_DESCR                                                             SSP_STATUS,
           LOCATION_CODE.CODE_DESCR                                                               CAMPUS,
           CS_SSP.AVAIL_YR                                                                        AVAILABILITY_YEAR,
           CS_SSP.SPK_NO                                                                          COURSE_SPK_NO,
           CS_SSP.AVAIL_KEY_NO                                                                    AVAIL_KEY_NO,
           CS_SSP.AVAIL_NO                                                                        AVAIL_NO,
           STUDY_MODE_CODE.CODE_DESCR                                                             STUDY_MODE,
           STUDY_TYPE_CODE.CODE_DESCR                                                             STUDY_TYPE,
           STUDY_BASIS_CODE.CODE_DESCR                                                            STUDY_BASIS,
           IFF(CS_SPK_DET.SPK_CAT_TYPE_CD in ('110', '130', '178', '122', '203'), 'Y', 'N')       HDR_COURSE,
           CASE
               WHEN COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE, START_DATE.EXPECTED_DATE) <
                    TRUNC(CURRENT_DATE, 'YYYY')
                   AND CURR_YEAR_ENR_SSP.SSP_NO IS NOT NULL
                   THEN 'Continuing'
               WHEN TRUNC(COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE, START_DATE.EXPECTED_DATE), 'YYYY') =
                    TRUNC(CURRENT_DATE, 'YYYY')
                   AND CURR_YEAR_ENR_SSP.SSP_NO IS NOT NULL
                   THEN 'Commencing'
               ELSE 'NA'
               END                                                                                COMMENCING_OR_CONTINUING,
           CAT_TYPE.SPK_CAT_LVL_CD                                                                COURSE_CAT_TYPE_LEVEL,
           UNIT_PASS_FAIL.FAIL_PASS_NUMBER                                                        UNIT_COMPLETE_ATTEMPT,
           UNIT_PASS_FAIL.FAIL_NUMBER                                                             UNIT_FAIL_NUMBER,
           UNIT_PASS_FAIL.PASS_NUMBER                                                             UNIT_PASS_NUMBER,
           IFF(CS_SSP.REPAR_SSP_NO > 0, 'Y', 'N')                                                 REPARENTED_COURSE,
           IFF(CS_SSP.SSP_STTS_CD IN ('PASS'), CS_SSP.EFFCT_START_DT, NULL)                       COURSE_COMPLETION_DATE,
           CONFERRAL_DATE.CONFERRAL_DT                                                            CONFERRAL_DATE,
           CURRENT_WAN.WAM                                                                        CURRENT_WAM,
           IFF(UN_ENROLLED.ENR_NO > 0, 'Y', 'N')                                                  UNIT_ENROLLED,
           UN_ENROLLED.ENR_NO                                                                     UNIT_ENROLLED_NUMBER,
           CS_SPK_DET.RPT_TO_GV1_FG                                                               GOV_REPORTED_FLAG,
           IFF(CURR_YEAR_ENR_SSP.SSP_NO IS NOT NULL, 'Y', 'N')                                    CURRENT_YEAR_ENROLMENT,
           IFF(CAT_TYPE.CRS_TYPE_CD IN ('42', '44'), 'Y', 'N')                                    CROSS_INSTITUTION_COURSE,
           OWN_ORG_UNIT.ORG_UNIT_CD                                                               COURSE_OWNING_ORG_UNIT_CODE,
           OWN_ORG_UNIT.ORG_UNIT_TYPE                                                             COURSE_OWNING_ORG_UNIT_TYPE,
           OWN_ORG_UNIT.ORG_UNIT_NM                                                               COURSE_OWNING_ORG_UNIT_NAME,
           SSP_DATE.EXPECTED_COMPLETION                                                           COURSE_EXPECTED_COMPLETION_DATE,
           SSP_DATE.START_DATE                                                                    COURSE_START_DATE,
           SSP_DATE.COURSE_OF_STUDY_START                                                         COURSE_OF_STUDY_START_DATE,
           SSP_DATE.MUST_COMPLETE_BY                                                              COURSE_MUST_COMPLETE_DATE,
           CS_SSP.STU_CONSOL_FG                                                                   CONSOLIDATED,
           CASE
               WHEN CALC_COURSE_ATTENDANCE.SSP_NO IS NOT NULL
                   THEN
                   CASE
                       WHEN CALC_COURSE_ATTENDANCE.MIN_ATTNDC_MODE_CD = CALC_COURSE_ATTENDANCE.MAX_ATTNDC_MODE_CD
                           THEN CASE
                                    WHEN CALC_COURSE_ATTENDANCE.MIN_ATTNDC_MODE_CD = 'INT' THEN 'Internal'
                                    ELSE 'External' END
                       ELSE 'Multi-modal'
                       END
               ELSE NULL END                                                                      CURRENT_ATTENDANCE_MODE,
           CAT_TYPE.CRS_TYPE_CD                                                                   COURSE_TYPE_CODE,
           CRS_TYPE_CODE.CODE_DESCR                                                               COURSE_TYPE_DESCRIPTION,
           FOE.PRIMARY_FIELD_OF_EDUCATION_CODE                                                    PRIMARY_FIELD_OF_EDUCATION_CODE,
           FOE.PRIMARY_FIELD_OF_EDUCATION_DESC                                                    PRIMARY_FIELD_OF_EDUCATION_DESC,
           FOE.SECONDARY_FIELD_OF_EDUCATION_CODE                                                  SECONDARY_FIELD_OF_EDUCATION_CODE,
           FOE.SECONDARY_FIELD_OF_EDUCATION_DESC                                                  SECONDARY_FIELD_OF_EDUCATION_DESC,
           CIRCOS_COURSE.CORRSPND_CD                                                              CRICOS_CODE,
           UN_FIRST_ENR.FIRST_ENROL_DATE                                                          FIRST_UNIT_ENROLLED_DATE,
           CASE
               WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '110' THEN 'PHD'
               WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '122' THEN 'Combined PHD'
               WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '178' THEN 'BPhil'
               WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '130' AND
                    UPPER(CS_SPK_DET.SPK_FULL_TITLE) LIKE 'MASTER OF PHILOSOPHY%'
                   THEN 'MPhil'
               WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '130' AND TIME_BASED_UNIT.PARENT_SSP_NO IS NULL
                   THEN 'Mres Y1'
               WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '130' AND TIME_BASED_UNIT.PARENT_SSP_NO IS NOT NULL
                   THEN 'Mres Y2'
               WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '203' THEN 'Exchange MRes'
               END                                                                                HDR_PROGRAM,
           COALESCE(FOE_SIX.CODE, '000000')                                                       FIELD_OF_EDUCATION_6_DIGIT_CODE,
           COALESCE(FOE_SIX.DESCRIPTION,
                    'Course is not a combined course or is a non-award course or is an OUA unit') FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
           COALESCE(FOE_FOUR.CODE, '0000')                                                        FIELD_OF_EDUCATION_4_DIGIT_CODE,
           COALESCE(FOE_FOUR.DESCRIPTION,
                    'Course is not a combined course or is a non-award course or is an OUA unit') FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
           COALESCE(FOE_TWO.CODE, '00')                                                           FIELD_OF_EDUCATION_2_DIGIT_CODE,
           COALESCE(FOE_TWO.DESCRIPTION,
                    'Course is not a combined course or is a non-award course or is an OUA unit') FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
           CS_SPK_DET.CRS_SPEC_CD                                                                 SPECIAL_COURSE_CODE,
           CS_SSP.EFFCT_START_DT                                                                  STATUS_EFFECTIVE_DATE,
           CS_SSP.SSP_STG_CD                                                                      STAGE_CODE,
           CS_SSP.SSP_STTS_CD                                                                     STATUS_CODE,
           IFF(CS_SSP.SSP_STTS_CD IN ('PASS', 'AWOL', 'WD', 'WDE', 'REPR', 'CANC'), CS_SSP.EFFCT_START_DT,
               NULL)                                                                              COURSE_END_DATE,
           COURSE_EFTSL.COURSE_EFTSL                                                              CURRENT_YEAR_EFTSL,
           'N'                                                                                    IS_DELETED,
           ACADEMIC_STATUS_CODE.CODE_ID                                                           ACADEMIC_STATUS,
           ACADEMIC_STATUS_CODE.CODE_DESCR                                                        ACADEMIC_STATUS_DESCRIPTION,
           OWN_ORG_UNIT_AVAIL.ORG_UNIT_CD                                                         COURSE_OWNING_ORG_UNIT_CODE_BY_AVAILABILITY,
           OWN_ORG_UNIT_AVAIL.ORG_UNIT_TYPE                                                       COURSE_OWNING_ORG_UNIT_TYPE_BY_AVAILABILITY,
           OWN_ORG_UNIT_AVAIL.ORG_UNIT_NM                                                         COURSE_OWNING_ORG_UNIT_NAME_BY_AVAILABILITY,
           MD5(
                       IFNULL(STUDENT_ID, '') || ',' ||
                       IFNULL(CS_SSP.PARENT_SSP_NO, 0) || ',' ||
                       IFNULL(ADMISSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                       IFNULL(ATTENDANCE_MODE, '') || ',' ||
                       IFNULL(ATTENDANCE_TYPE, '') || ',' ||
                       IFNULL(EQUIVALENT_YEAR_LEVEL, 0) || ',' ||
                       IFNULL(COURSE_LIABILITY_CATEGORY, '') || ',' ||
                       IFNULL(CURRENT_LIABILITY_CATEGORY, '') || ',' ||
                       IFNULL(COURSE_ANNUAL_CREDIT_POINTS, 0) || ',' ||
                       IFNULL(COURSE_TOTAL_CREDIT_POINTS, 0) || ',' ||
                       IFNULL(ADVANCED_STANDING_CREDIT_POINTS, 0) || ',' ||
                       IFNULL(CREDITED_CREDIT_POINTS, 0) || ',' ||
                       IFNULL(COMPLETED_CREDIT_POINTS, 0) || ',' ||
                       IFNULL(ENROLLED_CREDIT_POINTS, 0) || ',' ||
                       IFNULL(COURSE_CAT_TYPE_CODE, '') || ',' ||
                       IFNULL(COURSE_CAT_TYPE, '') || ',' ||
                       IFNULL(COURSE_CODE, '') || ',' ||
                       IFNULL(COURSE_VERSION, 0) || ',' ||
                       IFNULL(COURSE_FULL_TITLE, '') || ',' ||
                       IFNULL(CONSUMED_EFTSL, 0) || ',' ||
                       IFNULL(COURSE_TOTAL_EFTSL, 0) || ',' ||
                       IFNULL(APPLICATION_PATH, '') || ',' ||
                       IFNULL(SSP_STAGE, '') || ',' ||
                       IFNULL(SSP_STATUS, '') || ',' ||
                       IFNULL(CAMPUS, '') || ',' ||
                       IFNULL(AVAILABILITY_YEAR, 0) || ',' ||
                       IFNULL(COURSE_SPK_NO, 0) || ',' ||
                       IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                       IFNULL(CS_SSP.AVAIL_NO, 0) || ',' ||
                       IFNULL(STUDY_MODE, '') || ',' ||
                       IFNULL(STUDY_TYPE, '') || ',' ||
                       IFNULL(STUDY_BASIS, '') || ',' ||
                       IFNULL(HDR_COURSE, '') || ',' ||
                       IFNULL(COMMENCING_OR_CONTINUING, '') || ',' ||
                       IFNULL(COURSE_CAT_TYPE_LEVEL, '') || ',' ||
                       IFNULL(UNIT_COMPLETE_ATTEMPT, 0) || ',' ||
                       IFNULL(UNIT_FAIL_NUMBER, 0) || ',' ||
                       IFNULL(UNIT_PASS_NUMBER, 0) || ',' ||
                       IFNULL(REPARENTED_COURSE, '') || ',' ||
                       IFNULL(COURSE_COMPLETION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                       IFNULL(CONFERRAL_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                       IFNULL(CURRENT_WAM, 0) || ',' ||
                       IFNULL(UNIT_ENROLLED, '') || ',' ||
                       IFNULL(UNIT_ENROLLED_NUMBER, 0) || ',' ||
                       IFNULL(GOV_REPORTED_FLAG, '') || ',' ||
                       IFNULL(CURRENT_YEAR_ENROLMENT, '') || ',' ||
                       IFNULL(CROSS_INSTITUTION_COURSE, '') || ',' ||
                       IFNULL(COURSE_OWNING_ORG_UNIT_CODE, '') || ',' ||
                       IFNULL(COURSE_OWNING_ORG_UNIT_TYPE, '') || ',' ||
                       IFNULL(COURSE_OWNING_ORG_UNIT_NAME, '') || ',' ||
                       IFNULL(COURSE_EXPECTED_COMPLETION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                       IFNULL(COURSE_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                       IFNULL(COURSE_OF_STUDY_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                       IFNULL(COURSE_MUST_COMPLETE_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                       IFNULL(CONSOLIDATED, '') || ',' ||
                       IFNULL(CURRENT_ATTENDANCE_MODE, '') || ',' ||
                       IFNULL(COURSE_TYPE_CODE, '') || ',' ||
                       IFNULL(COURSE_TYPE_DESCRIPTION, '') || ',' ||
                       IFNULL(PRIMARY_FIELD_OF_EDUCATION_CODE, '') || ',' ||
                       IFNULL(PRIMARY_FIELD_OF_EDUCATION_DESC, '') || ',' ||
                       IFNULL(SECONDARY_FIELD_OF_EDUCATION_CODE, '') || ',' ||
                       IFNULL(SECONDARY_FIELD_OF_EDUCATION_DESC, '') || ',' ||
                       IFNULL(CRICOS_CODE, '') || ',' ||
                       IFNULL(FIRST_UNIT_ENROLLED_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                       IFNULL(HDR_PROGRAM, '') || ',' ||
                       IFNULL(FIELD_OF_EDUCATION_6_DIGIT_CODE, '') || ',' ||
                       IFNULL(FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION, '') || ',' ||
                       IFNULL(FIELD_OF_EDUCATION_4_DIGIT_CODE, '') || ',' ||
                       IFNULL(FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION, '') || ',' ||
                       IFNULL(FIELD_OF_EDUCATION_2_DIGIT_CODE, '') || ',' ||
                       IFNULL(FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION, '') || ',' ||
                       IFNULL(SPECIAL_COURSE_CODE, '') || ',' ||
                       IFNULL(STATUS_EFFECTIVE_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                       IFNULL(STAGE_CODE, '') || ',' ||
                       IFNULL(STATUS_CODE, '') || ',' ||
                       IFNULL(COURSE_END_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                       IFNULL(CURRENT_YEAR_EFTSL, 0) || ',' ||
                       IFNULL(IS_DELETED, '') || ',' ||
                       IFNULL(ACADEMIC_STATUS, '') || ',' ||
                       IFNULL(ACADEMIC_STATUS_DESCRIPTION, '') || ',' ||
                       IFNULL(COURSE_OWNING_ORG_UNIT_CODE_BY_AVAILABILITY, '') || ',' ||
                       IFNULL(COURSE_OWNING_ORG_UNIT_TYPE_BY_AVAILABILITY, '') || ',' ||
                       IFNULL(COURSE_OWNING_ORG_UNIT_NAME_BY_AVAILABILITY, '')
               )                                                                                  HASH_MD5
    FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
             JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                  ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                      AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
                      AND CS_SPK_DET.SPK_CAT_CD = 'CS'
             LEFT OUTER JOIN ODS.AMIS.S1SSP_DATE START_DATE
                             ON START_DATE.SSP_NO = CS_SSP.SSP_NO AND START_DATE.SSP_DT_TYPE_CD = 'STRT'
             LEFT OUTER JOIN ODS.AMIS.S1SSP_DATE COURSE_STUDY_START_DATE
                             ON COURSE_STUDY_START_DATE.SSP_NO = CS_SSP.SSP_NO AND
                                COURSE_STUDY_START_DATE.SSP_DT_TYPE_CD = 'COS'
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE ATTENDANCE_MODE_CODE
                             ON ATTENDANCE_MODE_CODE.CODE_ID = CS_SSP.ATTNDC_MODE_CD AND
                                ATTENDANCE_MODE_CODE.CODE_TYPE = 'ATTNDC_MODE_CD'
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE ACADEMIC_STATUS_CODE
                             ON ACADEMIC_STATUS_CODE.CODE_ID = CS_SSP.AC_STTS_CD AND
                                ACADEMIC_STATUS_CODE.CODE_TYPE = 'AC_STTS_CD'
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE ATTENDANCE_TYPE_CODE
                             ON ATTENDANCE_TYPE_CODE.CODE_ID = CS_SSP.LOAD_CAT_CD AND
                                ATTENDANCE_TYPE_CODE.CODE_TYPE = 'LOAD_CAT_CD'
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE COURSE_LIABILITY_CATEGORY_CODE
                             ON COURSE_LIABILITY_CATEGORY_CODE.CODE_ID = CS_SSP.LIAB_CAT_CD AND
                                COURSE_LIABILITY_CATEGORY_CODE.CODE_TYPE = 'LIAB_CAT_CD'
             LEFT OUTER JOIN CURRENT_LIABILITY
                             ON CURRENT_LIABILITY.SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE CURRENT_LIABILITY_CATEGORY_CODE
                             ON CURRENT_LIABILITY_CATEGORY_CODE.CODE_ID = CURRENT_LIABILITY.CURR_LIAB_CAT_CD AND
                                CURRENT_LIABILITY_CATEGORY_CODE.CODE_TYPE = 'LIAB_CAT_CD'
             LEFT OUTER JOIN ODS.AMIS.S1CSP_STUDY_MEASURE_DET CSP_STUDY_MEASURE_DET
                             ON CSP_STUDY_MEASURE_DET.STUDY_MEASURE_CD = CS_SPK_DET.DFLT_STUDY_MEASURE_CD
             LEFT OUTER JOIN ODS.AMIS.S1SPK_STUDY_MEASURE CS_SPK_STUDY_MEASURE
                             ON CS_SPK_STUDY_MEASURE.SPK_NO = CS_SPK_DET.SPK_NO AND
                                CS_SPK_STUDY_MEASURE.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO AND
                                CS_SPK_STUDY_MEASURE.STUDY_MEASURE_CD = CS_SPK_DET.DFLT_STUDY_MEASURE_CD
             LEFT OUTER JOIN ADVANCED_STANDING
                             ON ADVANCED_STANDING.PARENT_SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN CREDIT_POINTS
                             ON CREDIT_POINTS.PARENT_SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN ODS.AMIS.S1CAT_TYPE CAT_TYPE
                             ON CAT_TYPE.SPK_CAT_TYPE_CD = CS_SPK_DET.SPK_CAT_TYPE_CD
             LEFT OUTER JOIN HDR_CONSUMED_EFTSL
                             ON HDR_CONSUMED_EFTSL.SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN APPLICATION_PATH
                             ON APPLICATION_PATH.SSP_NO = CS_SSP.SSP_NO
                                 AND APPLICATION_PATH.STU_ID = CS_SSP.STU_ID
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SSP_STG_CODE
                             ON SSP_STG_CODE.CODE_ID = CS_SSP.SSP_STG_CD AND SSP_STG_CODE.CODE_TYPE = 'SSP_STG_CD'
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SSP_STATUS_CODE
                             ON SSP_STATUS_CODE.CODE_ID = CS_SSP.SSP_STTS_CD AND
                                SSP_STATUS_CODE.CODE_TYPE = 'SSP_STTS_CD'
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE LOCATION_CODE
                             ON LOCATION_CODE.CODE_ID = CS_SSP.LOCATION_CD AND LOCATION_CODE.CODE_TYPE = 'LOCATION'
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_MODE_CODE
                             ON STUDY_MODE_CODE.CODE_ID = CS_SSP.STUDY_MODE_CD AND
                                STUDY_MODE_CODE.CODE_TYPE = 'STUDY_MODE_CD'
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_TYPE_CODE
                             ON STUDY_TYPE_CODE.CODE_ID = CS_SSP.STUDY_TYPE_CD AND
                                STUDY_TYPE_CODE.CODE_TYPE = 'S1_STUDY_TYPE_CD'
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_BASIS_CODE
                             ON STUDY_BASIS_CODE.CODE_ID = CS_SSP.STUDY_BASIS_CD AND
                                STUDY_BASIS_CODE.CODE_TYPE = 'S1_STUDY_BASIS_CD'
             LEFT OUTER JOIN CURR_YEAR_ENR_SSP
                             ON CURR_YEAR_ENR_SSP.SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN UNIT_PASS_FAIL
                             ON UNIT_PASS_FAIL.SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN CONFERRAL_DATE
                             ON CONFERRAL_DATE.SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN UN_ENROLLED
                             ON UN_ENROLLED.SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN SSP_DATE
                             ON SSP_DATE.SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN SPK_OWN_ORG_UNIT
                             ON SPK_OWN_ORG_UNIT.SPK_NO = CS_SSP.SPK_NO AND
                                SPK_OWN_ORG_UNIT.SPK_VER_NO = CS_SSP.SPK_VER_NO
             LEFT OUTER JOIN OWN_ORG_UNIT
                             ON OWN_ORG_UNIT.ORG_UNIT_CD = IFF(SUBSTR(SPK_OWN_ORG_UNIT.ORG_UNIT_CD, 1, 1) = '8', '9011',
                                                               SUBSTR(SPK_OWN_ORG_UNIT.ORG_UNIT_CD, 1, 1) || '011')
             LEFT OUTER JOIN AVAIL_ORG AS AVAIL_ORG_AVAIL
                             ON AVAIL_ORG_AVAIL.AVAIL_KEY_NO=CS_SSP.AVAIL_KEY_NO
             LEFT OUTER JOIN OWN_ORG_UNIT AS OWN_ORG_UNIT_AVAIL
                             ON OWN_ORG_UNIT_AVAIL.ORG_UNIT_CD = AVAIL_ORG_AVAIL.ORG_UNIT_CD
             LEFT OUTER JOIN CURRENT_WAN
                             ON CURRENT_WAN.CS_SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN CALC_COURSE_ATTENDANCE
                             ON CALC_COURSE_ATTENDANCE.SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN ODS.AMIS.S1STC_CODE CRS_TYPE_CODE
                             ON CAT_TYPE.CRS_TYPE_CD = CRS_TYPE_CODE.CODE_ID AND
                                CRS_TYPE_CODE.CODE_TYPE = 'CRS_TYPE_CD'
             LEFT OUTER JOIN FOE
                             ON FOE.SPK_NO = CS_SSP.SPK_NO AND FOE.SPK_VER_NO = CS_SSP.SPK_VER_NO
             LEFT OUTER JOIN CIRCOS_COURSE
                             ON CIRCOS_COURSE.SPK_NO = CS_SPK_DET.SPK_NO AND
                                CIRCOS_COURSE.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
             LEFT OUTER JOIN UN_FIRST_ENR
                             ON UN_FIRST_ENR.CS_SSP = CS_SSP.SSP_NO
             LEFT OUTER JOIN TIME_BASED_UNIT
                             ON TIME_BASED_UNIT.PARENT_SSP_NO = CS_SSP.SSP_NO
             LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_SIX
                             ON FOE_SIX.CODE = LPAD(COALESCE(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, '0'), 6, '0')
             LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_FOUR
                             ON FOE_FOUR.CODE =
                                SUBSTR(LPAD(COALESCE(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, '0'), 6, '0'), 1, 4)
             LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_TWO
                             ON FOE_TWO.CODE =
                                SUBSTR(LPAD(COALESCE(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, '0'), 6, '0'), 1, 2)
             LEFT OUTER JOIN COURSE_EFTSL
                             ON COURSE_EFTSL.PARENT_SSP_NO = CS_SSP.SSP_NO

    WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_ADMISSION_SUM_SK,
       HUB_COURSE_ADMISSION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       B.HASH_MD5,
       B.STUDENT_ID,
       B.PARENT_SSP_NO,
       B.ADMISSION_DATE,
       B.ATTENDANCE_MODE,
       B.ATTENDANCE_TYPE,
       B.EQUIVALENT_YEAR_LEVEL,
       B.COURSE_LIABILITY_CATEGORY,
       B.CURRENT_LIABILITY_CATEGORY,
       B.COURSE_ANNUAL_CREDIT_POINTS,
       B.COURSE_TOTAL_CREDIT_POINTS,
       B.ADVANCED_STANDING_CREDIT_POINTS,
       B.CREDITED_CREDIT_POINTS,
       B.COMPLETED_CREDIT_POINTS,
       B.ENROLLED_CREDIT_POINTS,
       B.COURSE_CAT_TYPE_CODE,
       B.COURSE_CAT_TYPE,
       B.COURSE_CODE,
       B.COURSE_VERSION,
       B.COURSE_FULL_TITLE,
       B.CONSUMED_EFTSL,
       B.COURSE_TOTAL_EFTSL,
       B.APPLICATION_PATH,
       B.SSP_STAGE,
       B.SSP_STATUS,
       B.CAMPUS,
       B.AVAILABILITY_YEAR,
       B.COURSE_SPK_NO,
       B.AVAIL_KEY_NO,
       B.AVAIL_NO,
       B.STUDY_MODE,
       B.STUDY_TYPE,
       B.STUDY_BASIS,
       B.HDR_COURSE,
       B.COMMENCING_OR_CONTINUING,
       B.COURSE_CAT_TYPE_LEVEL,
       B.UNIT_COMPLETE_ATTEMPT,
       B.UNIT_FAIL_NUMBER,
       B.UNIT_PASS_NUMBER,
       B.REPARENTED_COURSE,
       B.COURSE_COMPLETION_DATE,
       B.CONFERRAL_DATE,
       B.CURRENT_WAM,
       B.UNIT_ENROLLED,
       B.UNIT_ENROLLED_NUMBER,
       B.GOV_REPORTED_FLAG,
       B.CURRENT_YEAR_ENROLMENT,
       B.CROSS_INSTITUTION_COURSE,
       B.COURSE_OWNING_ORG_UNIT_CODE,
       B.COURSE_OWNING_ORG_UNIT_TYPE,
       B.COURSE_OWNING_ORG_UNIT_NAME,
       B.COURSE_EXPECTED_COMPLETION_DATE,
       B.COURSE_START_DATE,
       B.COURSE_OF_STUDY_START_DATE,
       B.COURSE_MUST_COMPLETE_DATE,
       B.CONSOLIDATED,
       B.CURRENT_ATTENDANCE_MODE,
       B.COURSE_TYPE_CODE,
       B.COURSE_TYPE_DESCRIPTION,
       B.PRIMARY_FIELD_OF_EDUCATION_CODE,
       B.PRIMARY_FIELD_OF_EDUCATION_DESC,
       B.SECONDARY_FIELD_OF_EDUCATION_CODE,
       B.SECONDARY_FIELD_OF_EDUCATION_DESC,
       B.CRICOS_CODE,
       B.FIRST_UNIT_ENROLLED_DATE,
       B.HDR_PROGRAM,
       B.FIELD_OF_EDUCATION_6_DIGIT_CODE,
       B.FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
       B.FIELD_OF_EDUCATION_4_DIGIT_CODE,
       B.FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
       B.FIELD_OF_EDUCATION_2_DIGIT_CODE,
       B.FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
       B.SPECIAL_COURSE_CODE,
       B.STATUS_EFFECTIVE_DATE,
       B.STAGE_CODE,
       B.STATUS_CODE,
       B.COURSE_END_DATE,
       B.CURRENT_YEAR_EFTSL,
       B.IS_DELETED,
       B.ACADEMIC_STATUS,
       B.ACADEMIC_STATUS_DESCRIPTION,
       B.COURSE_OWNING_ORG_UNIT_CODE_BY_AVAILABILITY,
       B.COURSE_OWNING_ORG_UNIT_TYPE_BY_AVAILABILITY,
       B.COURSE_OWNING_ORG_UNIT_NAME_BY_AVAILABILITY
FROM SAT_ADMISSION_SUM B
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_COURSE_ADMISSION_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_ADMISSION_KEY ORDER BY LOAD_DTS) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_COURSE_ADMISSION_SUM) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_COURSE_ADMISSION_KEY = B.HUB_COURSE_ADMISSION_KEY
          AND S.HASH_MD5 = B.HASH_MD5
    );
