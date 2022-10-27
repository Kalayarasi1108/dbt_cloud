INSERT INTO DATA_VAULT.CORE.LNK_STUDENT_UNIT_ENROLMENT_TO_STRUCTURE (LNK_STUDENT_UNIT_ENROLMENT_TO_STRUCTURE_KEY,
                                                                     HUB_UNIT_ENROLMENT_KEY, HUB_DIRECT_STRUCTURE_KEY,
                                                                     HUB_DIRECT_MAJOR_KEY, STU_ID,
                                                                     UNIT_SPK_CD, UNIT_SPK_VER_NO, UNIT_AVAIL_YR,
                                                                     UNIT_LOCATION_CD, UNIT_SPRD_CD, UNIT_AVAIL_KEY_NO,
                                                                     UNIT_SSP_NO, UNIT_PARENT_SSP_NO,
                                                                     DIRECT_STRUCTURE_SPK_CD,
                                                                     DIRECT_STRUCTURE_SPK_VER_NO, DIRECT_MAJOR_SPK_CD,
                                                                     DIRECT_MAJOR_SPK_VER_NO, SOURCE, LOAD_DTS,
                                                                     ETL_JOB_ID)
SELECT MD5(IFNULL(STU_ID, '') || ',' ||
           IFNULL(UNIT_SPK_CD, '') || ',' ||
           IFNULL(UNIT_SPK_VER_NO, 0) || ',' ||
           IFNULL(UNIT_AVAIL_YR, 0) || ',' ||
           IFNULL(UNIT_LOCATION_CD, '') || ',' ||
           IFNULL(UNIT_SPRD_CD, '') || ',' ||
           IFNULL(UNIT_AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UNIT_SSP_NO, 0) || ',' ||
           IFNULL(UNIT_PARENT_SSP_NO, 0) || ',' ||
           IFNULL(DIRECT_STRUCTURE_SPK_CD, '') || ',' ||
           IFNULL(DIRECT_STRUCTURE_SPK_VER_NO, 0) || ',' ||
           IFNULL(DIRECT_MAJOR_SPK_CD, '') || ',' ||
           IFNULL(DIRECT_MAJOR_SPK_VER_NO, 0)
           )                                     LNK_STUDENT_UNIT_ENROLMENT_TO_STRUCTURE_KEY,
       MD5(IFNULL(STU_ID, '') || ',' ||
           IFNULL(UNIT_SPK_CD, '') || ',' ||
           IFNULL(UNIT_SPK_VER_NO, 0) || ',' ||
           IFNULL(UNIT_AVAIL_YR, 0) || ',' ||
           IFNULL(UNIT_LOCATION_CD, '') || ',' ||
           IFNULL(UNIT_SPRD_CD, '') || ',' ||
           IFNULL(UNIT_AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UNIT_SSP_NO, 0) || ',' ||
           IFNULL(UNIT_PARENT_SSP_NO, 0)
           )                                     HUB_UNIT_ENROLMENT_KEY,
       MD5(IFNULL(DIRECT_STRUCTURE_SPK_CD, '') || ',' ||
           IFNULL(DIRECT_STRUCTURE_SPK_VER_NO, 0)
           )                                     HUB_DIRECT_STRUCTURE_KEY,
       MD5(IFNULL(DIRECT_MAJOR_SPK_CD, '') || ',' ||
           IFNULL(DIRECT_MAJOR_SPK_VER_NO, 0)
           )                                     HUB_DIRECT_MAJOR_KEY,
       STU_ID,
       UNIT_SPK_CD,
       UNIT_SPK_VER_NO,
       UNIT_AVAIL_YR,
       UNIT_LOCATION_CD,
       UNIT_SPRD_CD,
       IFNULL(UNIT_AVAIL_KEY_NO, 0) UNIT_AVAIL_KEY_NO,
       UNIT_SSP_NO,
       UNIT_PARENT_SSP_NO,
       DIRECT_STRUCTURE_SPK_CD,
       DIRECT_STRUCTURE_SPK_VER_NO,
       DIRECT_MAJOR_SPK_CD,
       DIRECT_MAJOR_SPK_VER_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM (
         SELECT STU_ID,
                UNIT_SPK_CD,
                UNIT_SPK_VER_NO,
                UNIT_AVAIL_YR,
                UNIT_LOCATION_CD,
                UNIT_SPRD_CD,
                UNIT_AVAIL_KEY_NO,
                UNIT_SSP_NO,
                UNIT_PARENT_SSP_NO,
                -- direct structure
                STRUC_SPK_CD     DIRECT_STRUCTURE_SPK_CD,
                STRUC_SPK_VER_NO DIRECT_STRUCTURE_SPK_VER_NO,
                -- first major (MJ, MN, SP, ST) or last study set (SS)
                CASE
                    WHEN STRUC_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_SPK_CD
                    WHEN STRUC_1_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_1_SPK_CD
                    WHEN STRUC_2_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_2_SPK_CD
                    WHEN STRUC_3_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_3_SPK_CD
                    WHEN STRUC_4_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_4_SPK_CD
                    WHEN STRUC_5_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_5_SPK_CD
                    WHEN STRUC_6_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_6_SPK_CD
                    WHEN STRUC_6_SPK_CAT_CD = 'SS'
                        THEN STRUC_6_SPK_CD
                    WHEN STRUC_5_SPK_CAT_CD = 'SS'
                        THEN STRUC_5_SPK_CD
                    WHEN STRUC_4_SPK_CAT_CD = 'SS'
                        THEN STRUC_4_SPK_CD
                    WHEN STRUC_3_SPK_CAT_CD = 'SS'
                        THEN STRUC_3_SPK_CD
                    WHEN STRUC_2_SPK_CAT_CD = 'SS'
                        THEN STRUC_2_SPK_CD
                    WHEN STRUC_1_SPK_CAT_CD = 'SS'
                        THEN STRUC_1_SPK_CD
                    ELSE STRUC_SPK_CD
                    END          DIRECT_MAJOR_SPK_CD,
                CASE
                    WHEN STRUC_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_SPK_VER_NO
                    WHEN STRUC_1_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_1_SPK_VER_NO
                    WHEN STRUC_2_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_2_SPK_VER_NO
                    WHEN STRUC_3_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_3_SPK_VER_NO
                    WHEN STRUC_4_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_4_SPK_VER_NO
                    WHEN STRUC_5_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_5_SPK_VER_NO
                    WHEN STRUC_6_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_6_SPK_VER_NO
                    WHEN STRUC_6_SPK_CAT_CD = 'SS'
                        THEN STRUC_6_SPK_VER_NO
                    WHEN STRUC_5_SPK_CAT_CD = 'SS'
                        THEN STRUC_5_SPK_VER_NO
                    WHEN STRUC_4_SPK_CAT_CD = 'SS'
                        THEN STRUC_4_SPK_VER_NO
                    WHEN STRUC_3_SPK_CAT_CD = 'SS'
                        THEN STRUC_3_SPK_VER_NO
                    WHEN STRUC_2_SPK_CAT_CD = 'SS'
                        THEN STRUC_2_SPK_VER_NO
                    WHEN STRUC_1_SPK_CAT_CD = 'SS'
                        THEN STRUC_1_SPK_VER_NO
                    ELSE STRUC_SPK_VER_NO
                    END          DIRECT_MAJOR_SPK_VER_NO,
                CASE
                    WHEN STRUC_1_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_1_NOT_ON_PLAN_FG
                    WHEN STRUC_2_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_2_NOT_ON_PLAN_FG
                    WHEN STRUC_3_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_3_NOT_ON_PLAN_FG
                    WHEN STRUC_4_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_4_NOT_ON_PLAN_FG
                    WHEN STRUC_5_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_5_NOT_ON_PLAN_FG
                    WHEN STRUC_6_SPK_CAT_CD IN ('MJ', 'MN', 'ST', 'SP')
                        THEN STRUC_6_NOT_ON_PLAN_FG
                    WHEN STRUC_6_SPK_CAT_CD = 'SS'
                        THEN STRUC_6_NOT_ON_PLAN_FG
                    WHEN STRUC_5_SPK_CAT_CD = 'SS'
                        THEN STRUC_5_NOT_ON_PLAN_FG
                    WHEN STRUC_4_SPK_CAT_CD = 'SS'
                        THEN STRUC_4_NOT_ON_PLAN_FG
                    WHEN STRUC_3_SPK_CAT_CD = 'SS'
                        THEN STRUC_3_NOT_ON_PLAN_FG
                    WHEN STRUC_2_SPK_CAT_CD = 'SS'
                        THEN STRUC_2_NOT_ON_PLAN_FG
                    WHEN STRUC_1_SPK_CAT_CD = 'SS'
                        THEN STRUC_1_NOT_ON_PLAN_FG
                    ELSE NULL
                    END          DIRECT_MAJOR_NOT_ON_PLAN_FG
         FROM (
                  SELECT UN_SSP.STU_ID              STU_ID,
                         UN_SPK_DET.SPK_CD          UNIT_SPK_CD,
                         UN_SPK_DET.SPK_VER_NO      UNIT_SPK_VER_NO,
                         UN_SSP.AVAIL_YR            UNIT_AVAIL_YR,
                         UN_SSP.LOCATION_CD         UNIT_LOCATION_CD,
                         UN_SSP.SPRD_CD             UNIT_SPRD_CD,
                         UN_SSP.AVAIL_KEY_NO        UNIT_AVAIL_KEY_NO,
                         UN_SSP.SSP_NO              UNIT_SSP_NO,
                         CS_SSP.SSP_NO              UNIT_PARENT_SSP_NO,
                         STRUC_SPK.SPK_CD           STRUC_SPK_CD,
                         STRUC_SPK.SPK_VER_NO       STRUC_SPK_VER_NO,
                         STRUC_SPK.SPK_CAT_CD       STRUC_SPK_CAT_CD,
                         STRUC_SPK_1.SPK_CD         STRUC_1_SPK_CD,
                         STRUC_SPK_1.SPK_VER_NO     STRUC_1_SPK_VER_NO,
                         STRUC_SPK_1.SPK_CAT_CD     STRUC_1_SPK_CAT_CD,
                         STRUC_SSP_1.NOT_ON_PLAN_FG STRUC_1_NOT_ON_PLAN_FG,
                         STRUC_SPK_2.SPK_CD         STRUC_2_SPK_CD,
                         STRUC_SPK_2.SPK_VER_NO     STRUC_2_SPK_VER_NO,
                         STRUC_SPK_2.SPK_CAT_CD     STRUC_2_SPK_CAT_CD,
                         STRUC_SSP_2.NOT_ON_PLAN_FG STRUC_2_NOT_ON_PLAN_FG,
                         STRUC_SPK_3.SPK_CD         STRUC_3_SPK_CD,
                         STRUC_SPK_3.SPK_VER_NO     STRUC_3_SPK_VER_NO,
                         STRUC_SPK_3.SPK_CAT_CD     STRUC_3_SPK_CAT_CD,
                         STRUC_SSP_3.NOT_ON_PLAN_FG STRUC_3_NOT_ON_PLAN_FG,
                         STRUC_SPK_4.SPK_CD         STRUC_4_SPK_CD,
                         STRUC_SPK_4.SPK_VER_NO     STRUC_4_SPK_VER_NO,
                         STRUC_SPK_4.SPK_CAT_CD     STRUC_4_SPK_CAT_CD,
                         STRUC_SSP_4.NOT_ON_PLAN_FG STRUC_4_NOT_ON_PLAN_FG,
                         STRUC_SPK_5.SPK_CD         STRUC_5_SPK_CD,
                         STRUC_SPK_5.SPK_VER_NO     STRUC_5_SPK_VER_NO,
                         STRUC_SPK_5.SPK_CAT_CD     STRUC_5_SPK_CAT_CD,
                         STRUC_SSP_5.NOT_ON_PLAN_FG STRUC_5_NOT_ON_PLAN_FG,
                         STRUC_SPK_6.SPK_CD         STRUC_6_SPK_CD,
                         STRUC_SPK_6.SPK_VER_NO     STRUC_6_SPK_VER_NO,
                         STRUC_SPK_6.SPK_CAT_CD     STRUC_6_SPK_CAT_CD,
                         STRUC_SSP_6.NOT_ON_PLAN_FG STRUC_6_NOT_ON_PLAN_FG
                  FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                           JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                                ON CS_SPK_DET.SPK_CAT_CD = 'CS'
                                    AND CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                                    AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
                           JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                                ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO
                                    AND UN_SSP.SSP_NO != UN_SSP.PARENT_SSP_NO
                           JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
                                ON UN_SPK_DET.SPK_CAT_CD = 'UN'
                                    AND UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                                    AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SPK_DET STRUC_SPK
                                           ON STRUC_SPK.SPK_NO = UN_SSP.STRUCT_SPK_NO
                                               AND STRUC_SPK.SPK_VER_NO = UN_SSP.STRUCT_SPK_VER_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SSP_STU_SPK STRUC_SSP_1
                                           ON STRUC_SSP_1.SSP_NO = UN_SSP.STRUCT_FROM_SSP_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SPK_DET STRUC_SPK_1
                                           ON STRUC_SPK_1.SPK_NO = STRUC_SSP_1.STRUCT_SPK_NO
                                               AND STRUC_SPK_1.SPK_VER_NO =
                                                   STRUC_SSP_1.STRUCT_SPK_VER_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SSP_STU_SPK STRUC_SSP_2
                                           ON STRUC_SSP_2.SSP_NO = STRUC_SSP_1.STRUCT_FROM_SSP_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SPK_DET STRUC_SPK_2
                                           ON STRUC_SPK_2.SPK_NO = STRUC_SSP_2.STRUCT_SPK_NO
                                               AND STRUC_SPK_2.SPK_VER_NO =
                                                   STRUC_SSP_2.STRUCT_SPK_VER_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SSP_STU_SPK STRUC_SSP_3
                                           ON STRUC_SSP_3.SSP_NO = STRUC_SSP_2.STRUCT_FROM_SSP_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SPK_DET STRUC_SPK_3
                                           ON STRUC_SPK_3.SPK_NO = STRUC_SSP_3.STRUCT_SPK_NO
                                               AND STRUC_SPK_3.SPK_VER_NO =
                                                   STRUC_SSP_3.STRUCT_SPK_VER_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SSP_STU_SPK STRUC_SSP_4
                                           ON STRUC_SSP_4.SSP_NO = STRUC_SSP_3.STRUCT_FROM_SSP_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SPK_DET STRUC_SPK_4
                                           ON STRUC_SPK_4.SPK_NO = STRUC_SSP_4.STRUCT_SPK_NO
                                               AND STRUC_SPK_4.SPK_VER_NO =
                                                   STRUC_SSP_4.STRUCT_SPK_VER_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SSP_STU_SPK STRUC_SSP_5
                                           ON STRUC_SSP_5.SSP_NO = STRUC_SSP_4.STRUCT_FROM_SSP_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SPK_DET STRUC_SPK_5
                                           ON STRUC_SPK_5.SPK_NO = STRUC_SSP_5.STRUCT_SPK_NO
                                               AND STRUC_SPK_5.SPK_VER_NO =
                                                   STRUC_SSP_5.STRUCT_SPK_VER_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SSP_STU_SPK STRUC_SSP_6
                                           ON STRUC_SSP_6.SSP_NO = STRUC_SSP_5.STRUCT_FROM_SSP_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SPK_DET STRUC_SPK_6
                                           ON STRUC_SPK_6.SPK_NO = STRUC_SSP_6.STRUCT_SPK_NO
                                               AND STRUC_SPK_6.SPK_VER_NO =
                                                   STRUC_SSP_6.STRUCT_SPK_VER_NO
                  WHERE CS_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO
              )
     ) S
WHERE DIRECT_STRUCTURE_SPK_CD IS NOT NULL
 AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_STUDENT_UNIT_ENROLMENT_TO_STRUCTURE L
        WHERE L.LNK_STUDENT_UNIT_ENROLMENT_TO_STRUCTURE_KEY = MD5(IFNULL(S.STU_ID, '') || ',' ||
           IFNULL(S.UNIT_SPK_CD, '') || ',' ||
           IFNULL(S.UNIT_SPK_VER_NO, 0) || ',' ||
           IFNULL(S.UNIT_AVAIL_YR, 0) || ',' ||
           IFNULL(S.UNIT_LOCATION_CD, '') || ',' ||
           IFNULL(S.UNIT_SPRD_CD, '') || ',' ||
           IFNULL(S.UNIT_AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(S.UNIT_SSP_NO, 0) || ',' ||
           IFNULL(S.UNIT_PARENT_SSP_NO, 0) || ',' ||
           IFNULL(S.DIRECT_STRUCTURE_SPK_CD, '') || ',' ||
           IFNULL(S.DIRECT_STRUCTURE_SPK_VER_NO, 0) || ',' ||
           IFNULL(S.DIRECT_MAJOR_SPK_CD, '') || ',' ||
           IFNULL(S.DIRECT_MAJOR_SPK_VER_NO, 0)
           )
    )
;
