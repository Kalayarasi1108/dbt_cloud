INSERT INTO DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_SUM (SAT_UNIT_ENROLMENT_SUM_SK, HUB_UNIT_ENROLMENT_KEY, SOURCE, LOAD_DTS,
                                                    ETL_JOB_ID, HASH_MD5, STU_ID, UN_SSP_NO, UN_SPK_NO, UN_SPK_VER_NO,
                                                    UNIT_FULL_TITLE, UN_SPK_CD, UN_AVAIL_KEY_NO, UN_AVAIL_START_DATE,
                                                    UN_AVAIL_END_DATE, UN_AVAIL_CENSUS_DATE, UN_SPRD_CD,
                                                    UN_CREDIT_POINT,
                                                    COUNTABLE_UNIT_CREDIT_POINT, UN_CREDIT_MEASUREMENT_TYPE, CS_SSP_NO,
                                                    CS_SPK_NO, CS_SPK_CD, CS_SPK_VER_NO, CS_AVAIL_KEY_NO,
                                                    UN_SPK_CAT_TYPE_CD, UN_SPK_CAT_TYPE_DESC, UN_SPK_CAT_LVL_CD,
                                                    UN_SSP_STTS_NO, UN_SSP_STG_CD, UN_SSP_STAGE, UN_SSP_STTS_CD,
                                                    UN_SSP_STATUS, EFFCT_START_DT, NOT_ON_PLAN_FG, YR_LVL,
                                                    UN_LOCATION_CD, UN_CAMPUS, UN_AVAIL_YR, AVAIL_NO, AVAIL_KEY_NO,
                                                    LIAB_CAT_CD, LIABILITY_CATEGORY, LOAD_CAT_CD, LOAD_CATEGORY,
                                                    ATTNDC_MODE_CD, ATTENDANCE_MODE, STUDY_MODE_CD, STUDY_MODE,
                                                    THESIS_TITLE_DT, THESIS_TITLE, THESIS_ABSTRACT, GRADE_CD,
                                                    GRADE_TYPE_CD, PASS_FAIL_CD, MARK, MARK_COUNTABLE_IN_WAM,
                                                    VERIFIED_FG, RATIFIED_FG, RATIFIED_DT, RSLT_EFFCT_DT, RSLT_TYPE_CD,
                                                    FOE_CD, EXPECTED_SUBMISSION_DATE, FEC_LIAB_CAT_CD, CUR_LOCATION_CD,
                                                    CUR_EP_YEAR, CUR_EP_NO, STUDY_TYPE_CD, STUDY_TYPE, STUDY_BASIS_CD,
                                                    STUDY_BASIS, STUDY_PERIOD, CENSUS_DT, UN_GOVT_REPORT_DT,
                                                    UN_GOVT_REPORT_IND, UNIT_EFTSL, CURRENT_YEAR_CONSUMED_EFTSL,
                                                    UNIT_COMPLETE_ATTEMPT, UNIT_FAIL_NUMBER, UNIT_PASS_NUMBER,
                                                    FIRST_ENROL_DATE, OWNING_ORG_CODE, OWNING_ORG_NAME,
                                                    TEACHING_ORG_CODE_1, TEACHING_ORG_NAME_1, TEACHING_PERCENTAGE_1,
                                                    TEACHING_ORG_CODE_2, TEACHING_ORG_NAME_2, TEACHING_PERCENTAGE_2,
                                                    TEACHING_ORG_CODE_3, TEACHING_ORG_NAME_3, TEACHING_PERCENTAGE_3,
                                                    CURRENT_YEAR_EP_1_CONSUMED_EFTSL, CURRENT_YEAR_EP_2_CONSUMED_EFTSL,
                                                    STU_STTS_CD,
                                                    FIELD_OF_EDUCATION_6_DIGIT_CODE,
                                                    FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
                                                    FIELD_OF_EDUCATION_4_DIGIT_CODE,
                                                    FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
                                                    FIELD_OF_EDUCATION_2_DIGIT_CODE,
                                                    FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
                                                    CERTIFIED_FG,
                                                    CERTIFIED_DT,
                                                    PLAN_CMPNT_CD,
                                                    PLAN_COMPONENT_TYPE,
                                                    LAST_MODIFIED_DTS,
                                                    IS_DELETED,
                                                    COMMENCE_OR_CONTINUE,
                                                    REMITTED_DATE,
                                                    REMITTED_IND,
                                                    REPEAT_FAIL_IND)
WITH UN_SPK_AVAIL_DT AS (
    SELECT AVAIL_KEY_NO,
           START_DT,
           ROW_NUMBER() OVER (PARTITION BY AVAIL_KEY_NO ORDER BY DT_NO DESC) RN
    FROM ODS.AMIS.S1SPK_AVAIL_DT
    WHERE DT_TYPE_CD = 'TC'
)
   , CONSUMED_EFTSL AS (
    SELECT UN_SSP.SSP_NO,
           SUM(SSP_EP_DTL.GROSS_NORM_EFTSU + SSP_EP_DTL.GROSS_FEC_EFTSU) CONSUMED_EFTSL
    FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
             JOIN ODS.AMIS.S1SSP_EP_DTL SSP_EP_DTL
                  ON SSP_EP_DTL.SSP_NO = UN_SSP.SSP_NO
                      AND SSP_EP_DTL.EP_YEAR <= UN_SSP.CUR_EP_YEAR
                      AND SSP_EP_DTL.EP_CYR_RD_FG = 'Y'
    WHERE UN_SSP.SSP_NO != UN_SSP.PARENT_SSP_NO
      AND UN_SSP.STUDY_BASIS_CD = '$TIME'
    GROUP BY UN_SSP.SSP_NO
)
   , CURRENT_EP_YEAR_EFTSL AS (
    SELECT SSP_NO,
           SUM(
                   IFF(EP_START_DT < CURRENT_DATE,
                       GROSS_NORM_EFTSU + GROSS_NORM_EFTSU, NULL)) CONSUMED_EFTSL
    FROM ODS.AMIS.S1SSP_EP_DTL
    WHERE EP_YEAR = YEAR(CURRENT_DATE)::integer
      AND EP_CYR_RD_FG = 'Y'
      AND YEAR(CTL_CENSUS_DT) = YEAR(CURRENT_DATE)
    GROUP BY SSP_NO
)
   , CURRENT_YEAR_EP_EFTSL AS (
    SELECT SSP_NO,
           MAX(IFF(EP_NO = 1 AND EP_START_DT < CURRENT_DATE,
                   GROSS_NORM_EFTSU + GROSS_NORM_EFTSU, NULL)) CURRENT_YEAR_EP_1_CONSUMED_EFTSL,
           MAX(IFF(EP_NO = 2 AND EP_START_DT < CURRENT_DATE,
                   GROSS_NORM_EFTSU + GROSS_NORM_EFTSU, NULL)) CURRENT_YEAR_EP_2_CONSUMED_EFTSL
    FROM ODS.AMIS.S1SSP_EP_DTL
    WHERE EP_YEAR = YEAR(CURRENT_DATE)::INTEGER
      AND EP_CYR_RD_FG = 'Y'
      AND YEAR(CTL_CENSUS_DT) = YEAR(CURRENT_DATE)
    GROUP BY SSP_NO
)
   , UNIT_PASS_FAIL AS (
    SELECT UN_SSP_STTS_HIST.SSP_NO,
           SUM(IFF(UN_SSP_STTS_HIST.SSP_STTS_CD = 'FAIL', 1, 0))                FAIL_NUMBER,
           SUM(IFF(UN_SSP_STTS_HIST.SSP_STTS_CD = 'PASS', 1, 0))                PASS_NUMBER,
           SUM(IFF(UN_SSP_STTS_HIST.SSP_STTS_CD IN ('FAIL', 'PASS'), 1, 0))     FAIL_PASS_NUMBER,
           MIN(IFF(UN_SSP_STTS_HIST.SSP_STTS_CD = 'ENR', EFFCT_START_DT, NULL)) FIRST_ENROL_DATE
    FROM ODS.AMIS.S1SSP_STTS_HIST UN_SSP_STTS_HIST
    GROUP BY UN_SSP_STTS_HIST.SSP_NO
)
   , SPK_OWN_ORG_UNIT AS (
    SELECT SPK_NO, SPK_VER_NO, MAX(ORG_UNIT_CD) ORG_UNIT_CD
    FROM ODS.AMIS.S1SPK_ORG_UNIT
    WHERE RESP_CAT_CD = 'O'
    GROUP BY SPK_NO, SPK_VER_NO
)
   , SPK_OWN_AVAIL_ORG_UNIT AS (
    SELECT AVAIL_KEY_NO, MAX(ORG_UNIT_CD) ORG_UNIT_CD
    FROM ODS.AMIS.S1SPK_AVAIL_ORG
    WHERE RESP_CAT_CD = 'O'
    GROUP BY AVAIL_KEY_NO
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
   , SPK_TEACH_ORG_UNIT AS (
    SELECT A.SPK_NO,
           A.SPK_VER_NO,
           MAX(IFF(A.RN = 1, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_1,
           MAX(IFF(A.RN = 1, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_1,
           MAX(IFF(A.RN = 1, A.ORG_UNIT_NM, NULL)) TEACHING_ORG_NAME_1,
           MAX(IFF(A.RN = 2, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_2,
           MAX(IFF(A.RN = 2, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_2,
           MAX(IFF(A.RN = 2, A.ORG_UNIT_NM, NULL)) TEACHING_ORG_NAME_2,
           MAX(IFF(A.RN = 3, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_3,
           MAX(IFF(A.RN = 3, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_3,
           MAX(IFF(A.RN = 3, A.ORG_UNIT_NM, NULL)) TEACHING_ORG_NAME_3
    FROM (SELECT TEACH_SPK_ORG.SPK_NO,
                 TEACH_SPK_ORG.SPK_VER_NO,
                 TEACH_SPK_ORG.ORG_UNIT_CD,
                 TEACH_SPK_ORG.PCENT_RESP,
                 TEACH_ORG_UNIT.ORG_UNIT_NM,
                 TEACH_ORG_UNIT.ORG_UNIT_SHORT_NM,
                 ROW_NUMBER() OVER (PARTITION BY TEACH_SPK_ORG.SPK_NO, TEACH_SPK_ORG.SPK_VER_NO
                     ORDER BY TEACH_SPK_ORG.PCENT_RESP DESC, TEACH_SPK_ORG.ORG_UNIT_CD ASC) RN
          FROM ODS.AMIS.S1SPK_ORG_UNIT TEACH_SPK_ORG
                   JOIN (SELECT B.ORG_UNIT_CD,
                                B.ORG_UNIT_SHORT_NM,
                                B.ORG_UNIT_TYPE_CD,
                                B.ORG_UNIT_TYPE,
                                B.ORG_UNIT_NM
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
                              ) B
                         WHERE B.RN = 1) TEACH_ORG_UNIT
                        ON TEACH_ORG_UNIT.ORG_UNIT_CD = TEACH_SPK_ORG.ORG_UNIT_CD
          WHERE TEACH_SPK_ORG.RESP_CAT_CD = 'T'
         ) A
    GROUP BY A.SPK_NO, A.SPK_VER_NO
)
   , SPK_TEACH_AVAIL_ORG_UNIT AS (
    SELECT A.AVAIL_KEY_NO,
           MAX(IFF(A.RN = 1, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_1,
           MAX(IFF(A.RN = 1, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_1,
           MAX(IFF(A.RN = 1, A.ORG_UNIT_NM, NULL)) TEACHING_ORG_NAME_1,
           MAX(IFF(A.RN = 2, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_2,
           MAX(IFF(A.RN = 2, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_2,
           MAX(IFF(A.RN = 2, A.ORG_UNIT_NM, NULL)) TEACHING_ORG_NAME_2,
           MAX(IFF(A.RN = 3, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_3,
           MAX(IFF(A.RN = 3, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_3,
           MAX(IFF(A.RN = 3, A.ORG_UNIT_NM, NULL)) TEACHING_ORG_NAME_3
    FROM (SELECT TEACH_AVAIL_SPK_ORG.SPK_NO,
                 TEACH_AVAIL_SPK_ORG.SPK_VER_NO,
                 TEACH_AVAIL_SPK_ORG.AVAIL_KEY_NO,
                 TEACH_AVAIL_SPK_ORG.ORG_UNIT_CD,
                 TEACH_AVAIL_SPK_ORG.PCENT_RESP,
                 TEACH_ORG_UNIT.ORG_UNIT_NM,
                 TEACH_ORG_UNIT.ORG_UNIT_SHORT_NM,
                 ROW_NUMBER() OVER (PARTITION BY TEACH_AVAIL_SPK_ORG.AVAIL_KEY_NO
                     ORDER BY TEACH_AVAIL_SPK_ORG.PCENT_RESP DESC, TEACH_AVAIL_SPK_ORG.ORG_UNIT_CD ASC) RN
          FROM ODS.AMIS.S1SPK_AVAIL_ORG TEACH_AVAIL_SPK_ORG
                   JOIN (SELECT B.ORG_UNIT_CD,
                                B.ORG_UNIT_SHORT_NM,
                                B.ORG_UNIT_TYPE_CD,
                                B.ORG_UNIT_TYPE,
                                B.ORG_UNIT_NM
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
                              ) B
                         WHERE B.RN = 1) TEACH_ORG_UNIT
                        ON TEACH_ORG_UNIT.ORG_UNIT_CD = TEACH_AVAIL_SPK_ORG.ORG_UNIT_CD
          WHERE TEACH_AVAIL_SPK_ORG.RESP_CAT_CD = 'T'
         ) A
    GROUP BY A.AVAIL_KEY_NO
)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL SAT_UNIT_ENROLMENT_SUM_SK,
       UNIT_ENROLMENT_SUM.HUB_UNIT_ENROLMENT_KEY,
       UNIT_ENROLMENT_SUM.SOURCE,
       UNIT_ENROLMENT_SUM.LOAD_DTS,
       UNIT_ENROLMENT_SUM.ETL_JOB_ID,
       MD5(
                   IFNULL(UNIT_ENROLMENT_SUM.STU_ID, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_VER_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_AVAIL_KEY_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_AVAIL_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_AVAIL_END_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_TEACHING_CENSUS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SPRD_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_CREDIT_POINT, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.COUNTABLE_UNIT_CREDIT_POINT, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_CREDIT_MEASUREMENT_TYPE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CS_SSP_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CS_SPK_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CS_SPK_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CS_SPK_VER_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UNIT_FULL_TITLE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CS_AVAIL_KEY_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_CAT_TYPE_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_CAT_TYPE_DESC, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_CAT_LVL_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_STTS_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_STG_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_STAGE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_STTS_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_STATUS, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.EFFCT_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.NOT_ON_PLAN_FG, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.YR_LVL, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_LOCATION_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_CAMPUS, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_AVAIL_YR, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.AVAIL_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.AVAIL_KEY_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.LIAB_CAT_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.LIABILITY_CATEGORY, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.LOAD_CAT_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.ATTENDANCE_TYPE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.ATTNDC_MODE_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.ATTENDANCE_MODE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.STUDY_MODE_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.STUDY_MODE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.THESIS_TITLE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.THESIS_TITLE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.THESIS_ABSTRACT, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.GRADE_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.GRADE_TYPE_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.PASS_FAIL_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.MARK, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.MARK_COUNTABLE_IN_WAM, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.VERIFIED_FG, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.RATIFIED_FG, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.RATIFIED_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.RSLT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.RSLT_TYPE_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.FOE_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.EXPECTED_SUBMISSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.FEC_LIAB_CAT_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CUR_LOCATION_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CUR_EP_YEAR, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CUR_EP_NO, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.STUDY_TYPE_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.STUDY_TYPE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.STUDY_BASIS_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.STUDY_BASIS, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.STUDY_PERIOD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CENSUS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_GOVT_REPORT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UN_GOVT_REPORT_IND, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UNIT_EFTSL, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CURRENT_YEAR_CONSUMED_EFTSL, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UNIT_COMPLETE_ATTEMPT, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UNIT_FAIL_NUMBER, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.UNIT_PASS_NUMBER, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.FIRST_ENROL_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.OWNING_ORG_CODE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.OWNING_ORG_NAME, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_CODE_1, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_NAME_1, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.TEACHING_PERCENTAGE_1, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_CODE_2, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_NAME_2, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.TEACHING_PERCENTAGE_2, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_CODE_3, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_NAME_3, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.TEACHING_PERCENTAGE_3, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CURRENT_YEAR_EP_1_CONSUMED_EFTSL, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CURRENT_YEAR_EP_2_CONSUMED_EFTSL, 0) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.STU_STTS_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_6_DIGIT_CODE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_4_DIGIT_CODE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_2_DIGIT_CODE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CERTIFIED_FG, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.CERTIFIED_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.PLAN_COMPNT_CD, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.PLAN_COMPONENT_TYPE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.LAST_MODIFIED_DTS, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.IS_DELETED, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.COMMENCE_OR_CONTINUE, '') || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.REMITTED_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(UNIT_ENROLMENT_SUM.REMITTED_IND, '') || ',' ||
                   IFNULL(IFF(UNIT_ENROLMENT_SUM.FAILURE_COUNT>1, 'Y','N'),'')
           )                       HASH_MD5,
       UNIT_ENROLMENT_SUM.STU_ID,
       UNIT_ENROLMENT_SUM.UN_SSP_NO,
       UNIT_ENROLMENT_SUM.UN_SPK_NO,
       UNIT_ENROLMENT_SUM.UN_SPK_VER_NO,
       UNIT_ENROLMENT_SUM.UNIT_FULL_TITLE,
       UNIT_ENROLMENT_SUM.UN_SPK_CD,
       UNIT_ENROLMENT_SUM.UN_AVAIL_KEY_NO,
       UNIT_ENROLMENT_SUM.UN_AVAIL_START_DATE,
       UNIT_ENROLMENT_SUM.UN_AVAIL_END_DATE,
       UNIT_ENROLMENT_SUM.UN_TEACHING_CENSUS_DATE,
       UNIT_ENROLMENT_SUM.UN_SPRD_CD,
       UNIT_ENROLMENT_SUM.UN_CREDIT_POINT,
       UNIT_ENROLMENT_SUM.COUNTABLE_UNIT_CREDIT_POINT,
       UNIT_ENROLMENT_SUM.UN_CREDIT_MEASUREMENT_TYPE,
       UNIT_ENROLMENT_SUM.CS_SSP_NO,
       UNIT_ENROLMENT_SUM.CS_SPK_NO,
       UNIT_ENROLMENT_SUM.CS_SPK_CD,
       UNIT_ENROLMENT_SUM.CS_SPK_VER_NO,
       UNIT_ENROLMENT_SUM.CS_AVAIL_KEY_NO,
       UNIT_ENROLMENT_SUM.UN_SPK_CAT_TYPE_CD,
       UNIT_ENROLMENT_SUM.UN_SPK_CAT_TYPE_DESC,
       UNIT_ENROLMENT_SUM.UN_SPK_CAT_LVL_CD,
       UNIT_ENROLMENT_SUM.UN_SSP_STTS_NO,
       UNIT_ENROLMENT_SUM.UN_SSP_STG_CD,
       UNIT_ENROLMENT_SUM.UN_SSP_STAGE,
       UNIT_ENROLMENT_SUM.UN_SSP_STTS_CD,
       UNIT_ENROLMENT_SUM.UN_SSP_STATUS,
       UNIT_ENROLMENT_SUM.EFFCT_START_DT,
       UNIT_ENROLMENT_SUM.NOT_ON_PLAN_FG,
       UNIT_ENROLMENT_SUM.YR_LVL,
       UNIT_ENROLMENT_SUM.UN_LOCATION_CD,
       UNIT_ENROLMENT_SUM.UN_CAMPUS,
       UNIT_ENROLMENT_SUM.UN_AVAIL_YR,
       UNIT_ENROLMENT_SUM.AVAIL_NO,
       UNIT_ENROLMENT_SUM.AVAIL_KEY_NO,
       UNIT_ENROLMENT_SUM.LIAB_CAT_CD,
       UNIT_ENROLMENT_SUM.LIABILITY_CATEGORY,
       UNIT_ENROLMENT_SUM.LOAD_CAT_CD,
       UNIT_ENROLMENT_SUM.ATTENDANCE_TYPE,
       UNIT_ENROLMENT_SUM.ATTNDC_MODE_CD,
       UNIT_ENROLMENT_SUM.ATTENDANCE_MODE,
       UNIT_ENROLMENT_SUM.STUDY_MODE_CD,
       UNIT_ENROLMENT_SUM.STUDY_MODE,
       UNIT_ENROLMENT_SUM.THESIS_TITLE_DT,
       UNIT_ENROLMENT_SUM.THESIS_TITLE,
       UNIT_ENROLMENT_SUM.THESIS_ABSTRACT,
       UNIT_ENROLMENT_SUM.GRADE_CD,
       UNIT_ENROLMENT_SUM.GRADE_TYPE_CD,
       UNIT_ENROLMENT_SUM.PASS_FAIL_CD,
       UNIT_ENROLMENT_SUM.MARK,
       UNIT_ENROLMENT_SUM.MARK_COUNTABLE_IN_WAM,
       UNIT_ENROLMENT_SUM.VERIFIED_FG,
       UNIT_ENROLMENT_SUM.RATIFIED_FG,
       UNIT_ENROLMENT_SUM.RATIFIED_DT,
       UNIT_ENROLMENT_SUM.RSLT_EFFCT_DT,
       UNIT_ENROLMENT_SUM.RSLT_TYPE_CD,
       UNIT_ENROLMENT_SUM.FOE_CD,
       UNIT_ENROLMENT_SUM.EXPECTED_SUBMISSION_DATE,
       UNIT_ENROLMENT_SUM.FEC_LIAB_CAT_CD,
       UNIT_ENROLMENT_SUM.CUR_LOCATION_CD,
       UNIT_ENROLMENT_SUM.CUR_EP_YEAR,
       UNIT_ENROLMENT_SUM.CUR_EP_NO,
       UNIT_ENROLMENT_SUM.STUDY_TYPE_CD,
       UNIT_ENROLMENT_SUM.STUDY_TYPE,
       UNIT_ENROLMENT_SUM.STUDY_BASIS_CD,
       UNIT_ENROLMENT_SUM.STUDY_BASIS,
       UNIT_ENROLMENT_SUM.STUDY_PERIOD,
       UNIT_ENROLMENT_SUM.CENSUS_DT,
       UNIT_ENROLMENT_SUM.UN_GOVT_REPORT_DT,
       UNIT_ENROLMENT_SUM.UN_GOVT_REPORT_IND,
       UNIT_ENROLMENT_SUM.UNIT_EFTSL,
       UNIT_ENROLMENT_SUM.CURRENT_YEAR_CONSUMED_EFTSL,
       UNIT_ENROLMENT_SUM.UNIT_COMPLETE_ATTEMPT,
       UNIT_ENROLMENT_SUM.UNIT_FAIL_NUMBER,
       UNIT_ENROLMENT_SUM.UNIT_PASS_NUMBER,
       UNIT_ENROLMENT_SUM.FIRST_ENROL_DATE,
       UNIT_ENROLMENT_SUM.OWNING_ORG_CODE,
       UNIT_ENROLMENT_SUM.OWNING_ORG_NAME,
       UNIT_ENROLMENT_SUM.TEACHING_ORG_CODE_1,
       UNIT_ENROLMENT_SUM.TEACHING_ORG_NAME_1,
       UNIT_ENROLMENT_SUM.TEACHING_PERCENTAGE_1,
       UNIT_ENROLMENT_SUM.TEACHING_ORG_CODE_2,
       UNIT_ENROLMENT_SUM.TEACHING_ORG_NAME_2,
       UNIT_ENROLMENT_SUM.TEACHING_PERCENTAGE_2,
       UNIT_ENROLMENT_SUM.TEACHING_ORG_CODE_3,
       UNIT_ENROLMENT_SUM.TEACHING_ORG_NAME_3,
       UNIT_ENROLMENT_SUM.TEACHING_PERCENTAGE_3,
       UNIT_ENROLMENT_SUM.CURRENT_YEAR_EP_1_CONSUMED_EFTSL,
       UNIT_ENROLMENT_SUM.CURRENT_YEAR_EP_2_CONSUMED_EFTSL,
       UNIT_ENROLMENT_SUM.STU_STTS_CD,
       UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_6_DIGIT_CODE,
       UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
       UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_4_DIGIT_CODE,
       UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
       UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_2_DIGIT_CODE,
       UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
       UNIT_ENROLMENT_SUM.CERTIFIED_FG,
       UNIT_ENROLMENT_SUM.CERTIFIED_DT,
       UNIT_ENROLMENT_SUM.PLAN_COMPNT_CD,
       UNIT_ENROLMENT_SUM.PLAN_COMPONENT_TYPE,
       UNIT_ENROLMENT_SUM.LAST_MODIFIED_DTS,
       UNIT_ENROLMENT_SUM.IS_DELETED,
       UNIT_ENROLMENT_SUM.COMMENCE_OR_CONTINUE,
       UNIT_ENROLMENT_SUM.REMITTED_DATE,
       UNIT_ENROLMENT_SUM.REMITTED_IND,
       IFF(UNIT_ENROLMENT_SUM.FAILURE_COUNT>1, 'Y','N') REPEAT_FAIL_IND
FROM (
         SELECT MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                    IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                    IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                    IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                    IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                    IFNULL(UN_SSP.PARENT_SSP_NO, 0)
                    )                                                                                  HUB_UNIT_ENROLMENT_KEY,
                'AMIS'                                                                                 SOURCE,
                CURRENT_TIMESTAMP::timestamp_ntz                                                       LOAD_DTS,
                'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                              ETL_JOB_ID,
                UN_SSP.STU_ID                                                                          STU_ID,
                UN_SSP.SSP_NO                                                                          UN_SSP_NO,
                UN_SSP.SPK_NO                                                                          UN_SPK_NO,
                UN_SSP.SPK_VER_NO                                                                      UN_SPK_VER_NO,
                UN_SPK_DET.SPK_CD                                                                      UN_SPK_CD,
                UN_SSP.AVAIL_KEY_NO                                                                    UN_AVAIL_KEY_NO,
                UN_SPK_DET.SPK_FULL_TITLE                                                              UNIT_FULL_TITLE,
                UN_SPK_AVAIL_DET.START_DT                                                              UN_AVAIL_START_DATE,
                UN_SPK_AVAIL_DET.END_DT                                                                UN_AVAIL_END_DATE,
                UN_SPK_AVAIL_DT.START_DT                                                               UN_TEACHING_CENSUS_DATE,
                UN_SSP.SPRD_CD                                                                         UN_SPRD_CD,
                UN_SPK_STUDY_MEASURE.STUDY_MEASURE_VAL                                                 UN_CREDIT_POINT,
                IFF(UN_SSP.NOT_ON_PLAN_FG = 'Y', 'N', 'Y')                                             COUNTABLE_UNIT_CREDIT_POINT,
                UN_SPK_STUDY_MEASURE.STUDY_MEASURE_CD                                                  UN_CREDIT_MEASUREMENT_TYPE,
                UN_SSP.PARENT_SSP_NO                                                                   CS_SSP_NO,
                UN_SSP.PARENT_SPK_NO                                                                   CS_SPK_NO,
                CS_SPK_DET.SPK_CD                                                                      CS_SPK_CD,
                UN_SSP.PARENT_SPK_VER_NO                                                               CS_SPK_VER_NO,
                UN_SSP.PARENT_AVAIL_KEY                                                                CS_AVAIL_KEY_NO,
                UN_SPK_DET.SPK_CAT_TYPE_CD                                                             UN_SPK_CAT_TYPE_CD,
                UN_CAT_TYPE.SPK_CAT_TYPE_DESC                                                          UN_SPK_CAT_TYPE_DESC,
                UN_CAT_TYPE.SPK_CAT_LVL_CD                                                             UN_SPK_CAT_LVL_CD,
                UN_SSP.SSP_STTS_NO                                                                     UN_SSP_STTS_NO,
                UN_SSP.SSP_STG_CD                                                                      UN_SSP_STG_CD,
                SSP_STG_CODE.CODE_DESCR                                                                UN_SSP_STAGE,
                UN_SSP.SSP_STTS_CD                                                                     UN_SSP_STTS_CD,
                SSP_STATUS_CODE.CODE_DESCR                                                             UN_SSP_STATUS,
                UN_SSP.EFFCT_START_DT                                                                  EFFCT_START_DT,
                UN_SSP.NOT_ON_PLAN_FG                                                                  NOT_ON_PLAN_FG,
                UN_SSP.YR_LVL                                                                          YR_LVL,
                UN_SSP.LOCATION_CD                                                                     UN_LOCATION_CD,
                LOCATION_CODE.CODE_DESCR                                                               UN_CAMPUS,
                UN_SSP.AVAIL_YR                                                                        UN_AVAIL_YR,
                UN_SSP.AVAIL_NO                                                                        AVAIL_NO,
                UN_SSP.AVAIL_KEY_NO                                                                    AVAIL_KEY_NO,
                UN_SSP.LIAB_CAT_CD                                                                     LIAB_CAT_CD,
                UNIT_LIABILITY_CATEGORY_CODE.CODE_DESCR                                                LIABILITY_CATEGORY,
                UN_SSP.LOAD_CAT_CD                                                                     LOAD_CAT_CD,
                LOAD_CATEGORY_CODE.CODE_DESCR                                                          ATTENDANCE_TYPE,
                UN_SSP.ATTNDC_MODE_CD                                                                  ATTNDC_MODE_CD,
                ATTENDANCE_MODE_CODE.CODE_DESCR                                                        ATTENDANCE_MODE,
                UN_SSP.STUDY_MODE_CD                                                                   STUDY_MODE_CD,
                STUDY_MODE_CODE.CODE_DESCR                                                             STUDY_MODE,
                UN_SSP.THESIS_TITLE_DT                                                                 THESIS_TITLE_DT,
                UN_SSP.THESIS_TITLE                                                                    THESIS_TITLE,
                UN_SSP.THESIS_ABSTRACT                                                                 THESIS_ABSTRACT,
                UN_SSP.GRADE_CD                                                                        GRADE_CD,
                UN_SSP.GRADE_TYPE_CD                                                                   GRADE_TYPE_CD,
                UN_SSP.PASS_FAIL_CD                                                                    PASS_FAIL_CD,
                CASE
                    WHEN UN_SSP.PASS_FAIL_CD IN ('FN', 'PN', 'ON') AND MARK = 0
                        THEN NULL
                    WHEN
                        RSL_DET.MARKS_CD = '$Y' OR (RSL_DET.MARKS_CD = '$M' AND UN_SSP.MARK >= 0)
                        THEN UN_SSP.MARK
                    ELSE NULL END                                                                      MARK,
                CASE
                    WHEN UN_SSP.PASS_FAIL_CD IN ('FN', 'PN', 'ON') AND MARK = 0
                        THEN 'N'
                    WHEN UN_SSP.CERTIFIED_FG = 'Y' AND
                         UN_SSP.SSP_STTS_CD NOT IN ('CR', 'DES', 'EX') AND
                         (RSL_DET.MARKS_CD = '$Y' OR (RSL_DET.MARKS_CD = '$M' AND UN_SSP.MARK >= 0))
                        THEN 'Y'
                    ELSE 'N' END                                                                       MARK_COUNTABLE_IN_WAM,
                UN_SSP.VERIFIED_FG                                                                     VERIFIED_FG,
                UN_SSP.RATIFIED_FG                                                                     RATIFIED_FG,
                UN_SSP.RATIFIED_DT                                                                     RATIFIED_DT,
                UN_SSP.RSLT_EFFCT_DT                                                                   RSLT_EFFCT_DT,
                UN_SSP.RSLT_TYPE_CD                                                                    RSLT_TYPE_CD,
                UN_SSP.FOE_CD                                                                          FOE_CD,
                UN_SSP.LATEST_EWS_DT                                                                   EXPECTED_SUBMISSION_DATE,
                UN_SSP.FEC_LIAB_CAT_CD                                                                 FEC_LIAB_CAT_CD,
                UN_SSP.CUR_LOCATION_CD                                                                 CUR_LOCATION_CD,
                UN_SSP.CUR_EP_YEAR                                                                     CUR_EP_YEAR,
                UN_SSP.CUR_EP_NO                                                                       CUR_EP_NO,
                UN_SSP.STUDY_TYPE_CD                                                                   STUDY_TYPE_CD,
                STUDY_TYPE_CODE.CODE_DESCR                                                             STUDY_TYPE,
                UN_SSP.STUDY_BASIS_CD                                                                  STUDY_BASIS_CD,
                STUDY_BASIS_CODE.CODE_DESCR                                                            STUDY_BASIS,
                STUDY_PERIOD_CODE.CODE_DESCR                                                           STUDY_PERIOD,
                UN_SSP.CENSUS_DT                                                                       CENSUS_DT,
                UN_SSP.GOVT_REPORT_DT                                                                  UN_GOVT_REPORT_DT,
                UN_SSP.GOVT_REPORT_IND                                                                 UN_GOVT_REPORT_IND,
                CASE
                    WHEN CONSUMED_EFTSL.CONSUMED_EFTSL IS NOT NULL THEN CONSUMED_EFTSL.CONSUMED_EFTSL
                    WHEN YEAR(UN_SSP.CENSUS_DT) > 1900 AND CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL IS NOT NULL AND
                         CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL > 0
                        THEN UN_SPK_STUDY_MEASURE.STUDY_MEASURE_VAL / CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL
                    WHEN YEAR(UN_SSP.CENSUS_DT) > 1900 AND CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL IS NULL OR
                         CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL <= 0
                        THEN UN_SPK_STUDY_MEASURE.STUDY_MEASURE_VAL / CSP_STUDY_MEASURE_DET.ANNUAL_LOAD_VAL
                    ELSE 0
                    END                                                                                UNIT_EFTSL,
                CURRENT_EP_YEAR_EFTSL.CONSUMED_EFTSL                                                   CURRENT_YEAR_CONSUMED_EFTSL,
                UNIT_PASS_FAIL.FAIL_PASS_NUMBER                                                        UNIT_COMPLETE_ATTEMPT,
                UNIT_PASS_FAIL.FAIL_NUMBER                                                             UNIT_FAIL_NUMBER,
                UNIT_PASS_FAIL.PASS_NUMBER                                                             UNIT_PASS_NUMBER,
                UNIT_PASS_FAIL.FIRST_ENROL_DATE                                                        FIRST_ENROL_DATE,
                OWN_ORG_UNIT.ORG_UNIT_CD                                                               OWNING_ORG_CODE,
                OWN_ORG_UNIT.ORG_UNIT_NM                                                               OWNING_ORG_NAME,
                IFF(SPK_TEACH_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, SPK_TEACH_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_1,
                    SPK_TEACH_ORG_UNIT.TEACHING_ORG_CODE_1)                                            TEACHING_ORG_CODE_1,
                IFF(SPK_TEACH_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, SPK_TEACH_AVAIL_ORG_UNIT.TEACHING_ORG_NAME_1,
                    SPK_TEACH_ORG_UNIT.TEACHING_ORG_NAME_1)                                            TEACHING_ORG_NAME_1,
                IFF(SPK_TEACH_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, SPK_TEACH_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_1,
                    SPK_TEACH_ORG_UNIT.TEACHING_PERCENTAGE_1)                                          TEACHING_PERCENTAGE_1,
                IFF(SPK_TEACH_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, SPK_TEACH_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_2,
                    SPK_TEACH_ORG_UNIT.TEACHING_ORG_CODE_2)                                            TEACHING_ORG_CODE_2,
                IFF(SPK_TEACH_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, SPK_TEACH_AVAIL_ORG_UNIT.TEACHING_ORG_NAME_2,
                    SPK_TEACH_ORG_UNIT.TEACHING_ORG_NAME_2)                                            TEACHING_ORG_NAME_2,
                IFF(SPK_TEACH_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, SPK_TEACH_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_2,
                    SPK_TEACH_ORG_UNIT.TEACHING_PERCENTAGE_2)                                          TEACHING_PERCENTAGE_2,
                IFF(SPK_TEACH_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, SPK_TEACH_AVAIL_ORG_UNIT.TEACHING_ORG_CODE_3,
                    SPK_TEACH_ORG_UNIT.TEACHING_ORG_CODE_3)                                            TEACHING_ORG_CODE_3,
                IFF(SPK_TEACH_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, SPK_TEACH_AVAIL_ORG_UNIT.TEACHING_ORG_NAME_3,
                    SPK_TEACH_ORG_UNIT.TEACHING_ORG_NAME_3)                                            TEACHING_ORG_NAME_3,
                IFF(SPK_TEACH_AVAIL_ORG_UNIT.AVAIL_KEY_NO IS NOT NULL, SPK_TEACH_AVAIL_ORG_UNIT.TEACHING_PERCENTAGE_3,
                    SPK_TEACH_ORG_UNIT.TEACHING_PERCENTAGE_3)                                          TEACHING_PERCENTAGE_3,
                CURRENT_YEAR_EP_EFTSL.CURRENT_YEAR_EP_1_CONSUMED_EFTSL                                 CURRENT_YEAR_EP_1_CONSUMED_EFTSL,
                CURRENT_YEAR_EP_EFTSL.CURRENT_YEAR_EP_2_CONSUMED_EFTSL                                 CURRENT_YEAR_EP_2_CONSUMED_EFTSL,
                UN_SSP.STU_STTS_CD                                                                     STU_STTS_CD,
                COALESCE(FOE_SIX.CODE, '000000')                                                       FIELD_OF_EDUCATION_6_DIGIT_CODE,
                COALESCE(FOE_SIX.DESCRIPTION,
                         'Course is not a combined course or is a non-award course or is an OUA unit') FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
                COALESCE(FOE_FOUR.CODE, '0000')                                                        FIELD_OF_EDUCATION_4_DIGIT_CODE,
                COALESCE(FOE_FOUR.DESCRIPTION,
                         'Course is not a combined course or is a non-award course or is an OUA unit') FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
                COALESCE(FOE_TWO.CODE, '00')                                                           FIELD_OF_EDUCATION_2_DIGIT_CODE,
                COALESCE(FOE_TWO.DESCRIPTION,
                         'Course is not a combined course or is a non-award course or is an OUA unit') FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
                UN_SSP.CERTIFIED_FG,
                UN_SSP.CERTIFIED_DT,
                UN_SSP.PLAN_COMPNT_CD,
                PLAN_COMPONENT_TYPE_CODE.CODE_DESCR                                                    PLAN_COMPONENT_TYPE,
                TO_TIMESTAMP_NTZ(
                            TO_CHAR(UN_SSP.LAST_MOD_DATEI, 'YYYY-MM-DD') || ' ' || LPAD(UN_SSP.LAST_MOD_TIMEI, 6, '0'),
                            'YYYY-MM-DD HH24MISS')                                                     LAST_MODIFIED_DTS,
                'N'                                                                                    IS_DELETED,
                CASE
                    WHEN YEAR(UN_SSP.CENSUS_DT) =
                         YEAR(COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE, COURSE_START_DATE.EXPECTED_DATE))
                        THEN 'Commencing'
                    WHEN YEAR(UN_SSP.CENSUS_DT) >
                         YEAR(COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE, COURSE_START_DATE.EXPECTED_DATE))
                        THEN 'Continuing'
                    WHEN UN_SSP.SSP_STTS_CD IN ('ENR', 'PASS', 'FAIL') AND YEAR(UN_SSP.CENSUS_DT) = 1900
                        AND UN_SSP.STUDY_BASIS_CD = '$CRDT' AND UN_SSP.AVAIL_YR = YEAR(
                                COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE, COURSE_START_DATE.EXPECTED_DATE))
                        THEN 'Commencing'
                    WHEN UN_SSP.SSP_STTS_CD IN ('ENR', 'PASS', 'FAIL') AND YEAR(UN_SSP.CENSUS_DT) = 1900
                        AND UN_SSP.STUDY_BASIS_CD = '$CRDT' AND UN_SSP.AVAIL_YR > YEAR(
                                COALESCE(COURSE_STUDY_START_DATE.EXPECTED_DATE, COURSE_START_DATE.EXPECTED_DATE))
                        THEN 'Continuing'
                    ELSE 'Not Applicable'
                    END                                                                                "COMMENCE_OR_CONTINUE",
                UN_SSP.RMTD_DT                                                                         REMITTED_DATE,
                UN_SSP.RMTD_IND                                                                        REMITTED_IND,
                IFF(year(CENSUS_DT) = 1900,UN_TEACHING_CENSUS_DATE,CENSUS_DT)                          RF_CENSUS_DATE,
                IFF(UN_SSP_STATUS='Failed',RANK() OVER ( partition by UN_SSP.STU_ID,UN_SPK_DET.SPK_CD,SSP_STATUS_CODE.CODE_DESCR
               ORDER BY RF_CENSUS_DATE ASC),0) FAILURE_COUNT
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
                                      AND UN_SPK_STUDY_MEASURE.SPK_VER_NO = UN_SSP.SPK_VER_NO
                                      AND UN_SPK_STUDY_MEASURE.STUDY_MEASURE_CD = CS_SPK_DET.DFLT_STUDY_MEASURE_CD
                  LEFT OUTER JOIN ODS.AMIS.S1SPK_STUDY_MEASURE CS_SPK_STUDY_MEASURE
                                  ON CS_SPK_STUDY_MEASURE.SPK_NO = CS_SPK_DET.SPK_NO
                                      AND CS_SPK_STUDY_MEASURE.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
                                      AND CS_SPK_STUDY_MEASURE.STUDY_MEASURE_CD = CS_SPK_DET.DFLT_STUDY_MEASURE_CD
                  LEFT OUTER JOIN ODS.AMIS.S1CAT_TYPE UN_CAT_TYPE
                                  ON UN_CAT_TYPE.SPK_CAT_TYPE_CD = UN_SPK_DET.SPK_CAT_TYPE_CD
                  LEFT OUTER JOIN ODS.AMIS.S1SPK_AVAIL_DET UN_SPK_AVAIL_DET
                                  ON UN_SPK_AVAIL_DET.AVAIL_KEY_NO = UN_SSP.AVAIL_KEY_NO
                  LEFT OUTER JOIN UN_SPK_AVAIL_DT
                                  ON UN_SPK_AVAIL_DT.AVAIL_KEY_NO = UN_SPK_AVAIL_DET.AVAIL_KEY_NO
                                      AND UN_SPK_AVAIL_DT.RN = 1
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SSP_STG_CODE
                                  ON SSP_STG_CODE.CODE_ID = UN_SSP.SSP_STG_CD AND SSP_STG_CODE.CODE_TYPE = 'SSP_STG_CD'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SSP_STATUS_CODE
                                  ON SSP_STATUS_CODE.CODE_ID = UN_SSP.SSP_STTS_CD AND
                                     SSP_STATUS_CODE.CODE_TYPE = 'SSP_STTS_CD'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE LOCATION_CODE
                                  ON LOCATION_CODE.CODE_ID = UN_SSP.LOCATION_CD AND LOCATION_CODE.CODE_TYPE = 'LOCATION'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_MODE_CODE
                                  ON STUDY_MODE_CODE.CODE_ID = UN_SSP.STUDY_MODE_CD AND
                                     STUDY_MODE_CODE.CODE_TYPE = 'STUDY_MODE_CD'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_TYPE_CODE
                                  ON STUDY_TYPE_CODE.CODE_ID = UN_SSP.STUDY_TYPE_CD AND
                                     STUDY_TYPE_CODE.CODE_TYPE = 'S1_STUDY_TYPE_CD'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_BASIS_CODE
                                  ON STUDY_BASIS_CODE.CODE_ID = UN_SSP.STUDY_BASIS_CD AND
                                     STUDY_BASIS_CODE.CODE_TYPE = 'S1_STUDY_BASIS_CD'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE ATTENDANCE_MODE_CODE
                                  ON ATTENDANCE_MODE_CODE.CODE_ID = UN_SSP.ATTNDC_MODE_CD AND
                                     ATTENDANCE_MODE_CODE.CODE_TYPE = 'ATTNDC_MODE_CD'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE LOAD_CATEGORY_CODE
                                  ON LOAD_CATEGORY_CODE.CODE_ID = UN_SSP.LOAD_CAT_CD AND
                                     LOAD_CATEGORY_CODE.CODE_TYPE = 'LOAD_CAT_CD'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE UNIT_LIABILITY_CATEGORY_CODE
                                  ON UNIT_LIABILITY_CATEGORY_CODE.CODE_ID = UN_SSP.LIAB_CAT_CD AND
                                     UNIT_LIABILITY_CATEGORY_CODE.CODE_TYPE = 'LIAB_CAT_CD'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_PERIOD_CODE
                                  ON STUDY_PERIOD_CODE.CODE_ID = UN_SSP.SPRD_CD
                                      AND STUDY_PERIOD_CODE.CODE_TYPE = 'SPRD_CD'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE PLAN_COMPONENT_TYPE_CODE
                                  ON UN_SSP.PLAN_COMPNT_CD = PLAN_COMPONENT_TYPE_CODE.CODE_ID AND
                                     PLAN_COMPONENT_TYPE_CODE.CODE_TYPE = 'PLAN_CMPNT_CD'
                  LEFT OUTER JOIN CONSUMED_EFTSL
                                  ON CONSUMED_EFTSL.SSP_NO = UN_SSP.SSP_NO
                  LEFT OUTER JOIN CURRENT_EP_YEAR_EFTSL
                                  ON CURRENT_EP_YEAR_EFTSL.SSP_NO = UN_SSP.SSP_NO
                  LEFT OUTER JOIN CURRENT_YEAR_EP_EFTSL
                                  ON CURRENT_YEAR_EP_EFTSL.SSP_NO = UN_SSP.SSP_NO
                  LEFT OUTER JOIN UNIT_PASS_FAIL
                                  ON UNIT_PASS_FAIL.SSP_NO = UN_SSP.SSP_NO
                  LEFT OUTER JOIN SPK_OWN_ORG_UNIT
                                  ON SPK_OWN_ORG_UNIT.SPK_NO = UN_SSP.SPK_NO AND
                                     SPK_OWN_ORG_UNIT.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  LEFT OUTER JOIN SPK_OWN_AVAIL_ORG_UNIT
                                  ON SPK_OWN_AVAIL_ORG_UNIT.AVAIL_KEY_NO = UN_SSP.AVAIL_KEY_NO
                  LEFT OUTER JOIN OWN_ORG_UNIT
                                  ON OWN_ORG_UNIT.ORG_UNIT_CD =
                                     COALESCE(SPK_OWN_AVAIL_ORG_UNIT.ORG_UNIT_CD, SPK_OWN_ORG_UNIT.ORG_UNIT_CD)
                  LEFT OUTER JOIN SPK_TEACH_ORG_UNIT
                                  ON SPK_TEACH_ORG_UNIT.SPK_NO = UN_SSP.SPK_NO
                                      AND SPK_TEACH_ORG_UNIT.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  LEFT OUTER JOIN SPK_TEACH_AVAIL_ORG_UNIT
                                  ON SPK_TEACH_AVAIL_ORG_UNIT.AVAIL_KEY_NO = UN_SSP.AVAIL_KEY_NO
                  LEFT OUTER JOIN ODS.AMIS.S1CSP_STUDY_MEASURE_DET CSP_STUDY_MEASURE_DET
                                  ON CSP_STUDY_MEASURE_DET.STUDY_MEASURE_CD =
                                     CASE
                                         WHEN TRIM(CS_SPK_DET.DFLT_STUDY_MEASURE_CD) != '' AND
                                              CS_SPK_DET.DFLT_STUDY_MEASURE_CD IS NOT NULL
                                             THEN CS_SPK_DET.DFLT_STUDY_MEASURE_CD
                                         WHEN LENGTH(CS_SPK_DET.SPK_CD) < 8 THEN '$CP'
                                         ELSE 'CP2'
                                         END
                  LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_SIX
                                  ON FOE_SIX.CODE = LPAD(COALESCE(UN_SSP.FOE_CD, '0'), 6, '0')
                  LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_FOUR
                                  ON FOE_FOUR.CODE =
                                     SUBSTR(LPAD(COALESCE(UN_SSP.FOE_CD, '0'), 6, '0'), 1, 4)
                  LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_TWO
                                  ON FOE_TWO.CODE =
                                     SUBSTR(LPAD(COALESCE(UN_SSP.FOE_CD, '0'), 6, '0'), 1, 2)
                  LEFT OUTER JOIN ODS.AMIS.S1RSL_DET RSL_DET
                                  ON RSL_DET.RSLT_TYPE_CD = UN_SSP.RSLT_TYPE_CD
                                      AND RSL_DET.GRADE_CD = UN_SSP.GRADE_CD
                                      AND UN_SSP.RSLT_EFFCT_DT >= RSL_DET.EFFCT_DT
                                      AND UN_SSP.RSLT_EFFCT_DT <=
                                          IFF(YEAR(RSL_DET.EXPIRY_DT) = 1900, TO_DATE('2050', 'YYYY'),
                                              RSL_DET.EXPIRY_DT)
                  LEFT OUTER JOIN ODS.AMIS.S1SSP_DATE COURSE_START_DATE
                                  ON COURSE_START_DATE.SSP_NO = UN_SSP.PARENT_SSP_NO and
                                     COURSE_START_DATE.SSP_DT_TYPE_CD = 'STRT'
                  LEFT OUTER JOIN ODS.AMIS.S1SSP_DATE COURSE_STUDY_START_DATE
                                  ON COURSE_STUDY_START_DATE.SSP_NO = UN_SSP.PARENT_SSP_NO AND
                                     COURSE_STUDY_START_DATE.SSP_DT_TYPE_CD = 'COS'
         WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
           AND UN_SSP.STU_CONSOL_FG = 'N'
     ) UNIT_ENROLMENT_SUM
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_UNIT_ENROLMENT_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_UNIT_ENROLMENT_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_SUM) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_UNIT_ENROLMENT_KEY = MD5(IFNULL(UNIT_ENROLMENT_SUM.STU_ID, '') || ',' ||
                                             IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_CD, '') || ',' ||
                                             IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_VER_NO, 0) || ',' ||
                                             IFNULL(UNIT_ENROLMENT_SUM.UN_AVAIL_YR, 0) || ',' ||
                                             IFNULL(UNIT_ENROLMENT_SUM.UN_LOCATION_CD, '') || ',' ||
                                             IFNULL(UNIT_ENROLMENT_SUM.UN_SPRD_CD, '') || ',' ||
                                             IFNULL(UNIT_ENROLMENT_SUM.UN_AVAIL_KEY_NO, 0) || ',' ||
                                             IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_NO, 0) || ',' ||
                                             IFNULL(UNIT_ENROLMENT_SUM.CS_SSP_NO, 0)
            )
          AND S.HASH_MD5 = MD5(
                    IFNULL(UNIT_ENROLMENT_SUM.STU_ID, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_VER_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_AVAIL_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_AVAIL_END_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_TEACHING_CENSUS_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SPRD_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_CREDIT_POINT, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.COUNTABLE_UNIT_CREDIT_POINT, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_CREDIT_MEASUREMENT_TYPE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CS_SSP_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CS_SPK_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CS_SPK_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CS_SPK_VER_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UNIT_FULL_TITLE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CS_AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_CAT_TYPE_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_CAT_TYPE_DESC, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SPK_CAT_LVL_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_STTS_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_STG_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_STAGE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_STTS_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_SSP_STATUS, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.EFFCT_START_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.NOT_ON_PLAN_FG, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.YR_LVL, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_LOCATION_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_CAMPUS, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_AVAIL_YR, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.AVAIL_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.LIAB_CAT_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.LIABILITY_CATEGORY, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.LOAD_CAT_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.ATTENDANCE_TYPE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.ATTNDC_MODE_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.ATTENDANCE_MODE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.STUDY_MODE_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.STUDY_MODE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.THESIS_TITLE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.THESIS_TITLE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.THESIS_ABSTRACT, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.GRADE_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.GRADE_TYPE_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.PASS_FAIL_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.MARK, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.MARK_COUNTABLE_IN_WAM, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.VERIFIED_FG, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.RATIFIED_FG, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.RATIFIED_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.RSLT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.RSLT_TYPE_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.FOE_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.EXPECTED_SUBMISSION_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.FEC_LIAB_CAT_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CUR_LOCATION_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CUR_EP_YEAR, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CUR_EP_NO, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.STUDY_TYPE_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.STUDY_TYPE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.STUDY_BASIS_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.STUDY_BASIS, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.STUDY_PERIOD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CENSUS_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_GOVT_REPORT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UN_GOVT_REPORT_IND, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UNIT_EFTSL, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CURRENT_YEAR_CONSUMED_EFTSL, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UNIT_COMPLETE_ATTEMPT, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UNIT_FAIL_NUMBER, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.UNIT_PASS_NUMBER, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.FIRST_ENROL_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.OWNING_ORG_CODE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.OWNING_ORG_NAME, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_CODE_1, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_NAME_1, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.TEACHING_PERCENTAGE_1, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_CODE_2, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_NAME_2, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.TEACHING_PERCENTAGE_2, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_CODE_3, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.TEACHING_ORG_NAME_3, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.TEACHING_PERCENTAGE_3, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CURRENT_YEAR_EP_1_CONSUMED_EFTSL, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CURRENT_YEAR_EP_2_CONSUMED_EFTSL, 0) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.STU_STTS_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_6_DIGIT_CODE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_4_DIGIT_CODE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_2_DIGIT_CODE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CERTIFIED_FG, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.CERTIFIED_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.PLAN_COMPNT_CD, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.PLAN_COMPONENT_TYPE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.LAST_MODIFIED_DTS, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.IS_DELETED, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.COMMENCE_OR_CONTINUE, '') || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.REMITTED_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(UNIT_ENROLMENT_SUM.REMITTED_IND, '') || ',' ||
                    IFNULL(IFF(UNIT_ENROLMENT_SUM.FAILURE_COUNT>1, 'Y','N'),'')
            )
    );
