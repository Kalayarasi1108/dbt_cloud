INSERT INTO DATA_VAULT.CORE.SAT_COURSE_AMIS (SAT_COURSE_AMIS_SK, HUB_COURSE_KEY, SOURCE,
                                                      LOAD_DTS, ETL_JOB_ID, HASH_MD5, COURSE_SPK_NO, COURSE_VERSION,
                                                      COURSE_CODE, COURSE_SORT_CODE,
                                                      COURSE_CAT_TYPE_CODE, COURSE_STAGE_CODE, COURSE_STAGE,
                                                      COURSE_FULL_TITLE, COURSE_SHORT_TITLE, COURSE_ABBREVIATED_TITLE,
                                                      EFFECTIVE_DATE, DISCONTINUED_DATE, STRUCTURE_FLAG,
                                                      GOV_REPORTED_FLAG, RPT_TO_GV3_FG, RPT_TO_GV4_FG, COMBINED_COURSE,
                                                      COURSE_CREATION_DTS, COURSE_LAST_MODIFIED_DTS,
                                                      MAXIMUM_GOVERNMENT_FUNDED_EFTSU, EXPECTED_EFTSU_TO_COMPLETE,
                                                      EXPECTED_TIME_TO_COMPLETE, MINIMUM_EFTSU_TO_COMPLETE,
                                                      MAXIMUM_EFTSU_TO_COMPLETE, COURSE_ANNUAL_CREDIT_POINTS,
                                                      HDR_COURSE, COURSE_CAT_TYPE_LEVEL, CROSS_INSTITUTION_COURSE,
                                                      COURSE_OWNING_ORG_UNIT_CODE, COURSE_OWNING_ORG_UNIT_TYPE,
                                                      COURSE_OWNING_ORG_UNIT_NAME,
                                                      COURSE_TYPE_CODE, COURSE_TYPE_DESCRIPTION, LEVEL_OF_STUDY,
                                                      PRIMARY_FIELD_OF_EDUCATION_CODE, PRIMARY_FIELD_OF_EDUCATION_DESC,
                                                      SECONDARY_FIELD_OF_EDUCATION_CODE,
                                                      SECONDARY_FIELD_OF_EDUCATION_DESC, CRICOS_CODE, HDR_PROGRAM,
                                                      FIELD_OF_EDUCATION_6_DIGIT_CODE,
                                                      FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
                                                      FIELD_OF_EDUCATION_4_DIGIT_CODE,
                                                      FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
                                                      FIELD_OF_EDUCATION_2_DIGIT_CODE,
                                                      FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
                                                      SPECIAL_COURSE_CODE,
                                                      IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_AMIS_SK,
       MD5(
                   IFNULL(A.COURSE_CODE, '') || ',' ||
                   IFNULL(A.COURSE_VERSION, 0)
           )                                     HUB_COURSE_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(COURSE_SPK_NO, 0) || ',' ||
           IFNULL(COURSE_VERSION, 0) || ',' ||
           IFNULL(COURSE_CODE, '') || ',' ||
           IFNULL(COURSE_SORT_CODE, '') || ',' ||
           IFNULL(COURSE_CAT_TYPE_CODE, '') || ',' ||
           IFNULL(COURSE_STAGE_CODE, '') || ',' ||
           IFNULL(COURSE_STAGE, '') || ',' ||
           IFNULL(COURSE_FULL_TITLE, '') || ',' ||
           IFNULL(COURSE_SHORT_TITLE, '') || ',' ||
           IFNULL(COURSE_ABBREVIATED_TITLE, '') || ',' ||
           IFNULL(EFFECTIVE_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(DISCONTINUED_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STRUCTURE_FLAG, '') || ',' ||
           IFNULL(GOV_REPORTED_FLAG, '') || ',' ||
           IFNULL(RPT_TO_GV3_FG, '') || ',' ||
           IFNULL(RPT_TO_GV4_FG, '') || ',' ||
           IFNULL(COMBINED_COURSE, '') || ',' ||
           IFNULL(COURSE_CREATION_DTS, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(COURSE_LAST_MODIFIED_DTS, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(MAXIMUM_GOVERNMENT_FUNDED_EFTSU, 0) || ',' ||
           IFNULL(EXPECTED_EFTSU_TO_COMPLETE, 0) || ',' ||
           IFNULL(EXPECTED_TIME_TO_COMPLETE, 0) || ',' ||
           IFNULL(MINIMUM_EFTSU_TO_COMPLETE, 0) || ',' ||
           IFNULL(MAXIMUM_EFTSU_TO_COMPLETE, 0) || ',' ||
           IFNULL(COURSE_ANNUAL_CREDIT_POINTS, 0) || ',' ||
           IFNULL(HDR_COURSE, '') || ',' ||
           IFNULL(COURSE_CAT_TYPE_LEVEL, '') || ',' ||
           IFNULL(CROSS_INSTITUTION_COURSE, '') || ',' ||
           IFNULL(COURSE_OWNING_ORG_UNIT_CODE, '') || ',' ||
           IFNULL(COURSE_OWNING_ORG_UNIT_TYPE, '') || ',' ||
           IFNULL(COURSE_OWNING_ORG_UNIT_NAME, '') || ',' ||
           IFNULL(COURSE_TYPE_CODE, '') || ',' ||
           IFNULL(COURSE_TYPE_DESCRIPTION, '') || ',' ||
           IFNULL(LEVEL_OF_STUDY, '') || ',' ||
           IFNULL(PRIMARY_FIELD_OF_EDUCATION_CODE, '') || ',' ||
           IFNULL(PRIMARY_FIELD_OF_EDUCATION_DESC, '') || ',' ||
           IFNULL(SECONDARY_FIELD_OF_EDUCATION_CODE, '') || ',' ||
           IFNULL(SECONDARY_FIELD_OF_EDUCATION_DESC, '') || ',' ||
           IFNULL(CRICOS_CODE, '') || ',' ||
           IFNULL(HDR_PROGRAM, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_6_DIGIT_CODE, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_4_DIGIT_CODE, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_2_DIGIT_CODE, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION, '') || ',' ||
           IFNULL(SPECIAL_COURSE_CODE, '') || ',' ||
           IFNULL('N', '')
           )                                     HASH_MD5,
       A.COURSE_SPK_NO,
       A.COURSE_VERSION,
       A.COURSE_CODE,
       A.COURSE_SORT_CODE,
       A.COURSE_CAT_TYPE_CODE,
       A.COURSE_STAGE_CODE,
       A.COURSE_STAGE,
       A.COURSE_FULL_TITLE,
       A.COURSE_SHORT_TITLE,
       A.COURSE_ABBREVIATED_TITLE,
       A.EFFECTIVE_DATE,
       A.DISCONTINUED_DATE,
       A.STRUCTURE_FLAG,
       A.GOV_REPORTED_FLAG,
       A.RPT_TO_GV3_FG,
       A.RPT_TO_GV4_FG,
       A.COMBINED_COURSE,
       A.COURSE_CREATION_DTS,
       A.COURSE_LAST_MODIFIED_DTS,
       A.MAXIMUM_GOVERNMENT_FUNDED_EFTSU,
       A.EXPECTED_EFTSU_TO_COMPLETE,
       A.EXPECTED_TIME_TO_COMPLETE,
       A.MINIMUM_EFTSU_TO_COMPLETE,
       A.MAXIMUM_EFTSU_TO_COMPLETE,
       A.COURSE_ANNUAL_CREDIT_POINTS,
       A.HDR_COURSE,
       A.COURSE_CAT_TYPE_LEVEL,
       A.CROSS_INSTITUTION_COURSE,
       A.COURSE_OWNING_ORG_UNIT_CODE,
       A.COURSE_OWNING_ORG_UNIT_TYPE,
       A.COURSE_OWNING_ORG_UNIT_NAME,
       A.COURSE_TYPE_CODE,
       A.COURSE_TYPE_DESCRIPTION,
       A.LEVEL_OF_STUDY,
       A.PRIMARY_FIELD_OF_EDUCATION_CODE,
       A.PRIMARY_FIELD_OF_EDUCATION_DESC,
       A.SECONDARY_FIELD_OF_EDUCATION_CODE,
       A.SECONDARY_FIELD_OF_EDUCATION_DESC,
       A.CRICOS_CODE,
       A.HDR_PROGRAM,
       A.FIELD_OF_EDUCATION_6_DIGIT_CODE,
       A.FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION,
       A.FIELD_OF_EDUCATION_4_DIGIT_CODE,
       A.FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION,
       A.FIELD_OF_EDUCATION_2_DIGIT_CODE,
       A.FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION,
       A.SPECIAL_COURSE_CODE,
       'N'                                       IS_DELETED
FROM (SELECT CS_SPK_DET.SPK_NO                                                                      COURSE_SPK_NO,
             CS_SPK_DET.SPK_VER_NO                                                                  COURSE_VERSION,
             CS_SPK_DET.SPK_CD                                                                      COURSE_CODE,
             CS_SPK_DET.SPK_SORT_CD                                                                 COURSE_SORT_CODE,
             CS_SPK_DET.SPK_CAT_TYPE_CD                                                             COURSE_CAT_TYPE_CODE,
             CS_SPK_DET.SPK_STAGE_CD                                                                COURSE_STAGE_CODE,
             SPK_STAGE_CODE.CODE_DESCR                                                              COURSE_STAGE,
             CS_SPK_DET.SPK_FULL_TITLE                                                              COURSE_FULL_TITLE,
             CS_SPK_DET.SPK_SHORT_TITLE                                                             COURSE_SHORT_TITLE,
             CS_SPK_DET.SPK_ABBR_TITLE                                                              COURSE_ABBREVIATED_TITLE,
             CS_SPK_DET.EFFCT_DT                                                                    EFFECTIVE_DATE,
             CS_SPK_DET.DISCNT_DATE                                                                 DISCONTINUED_DATE,
             CS_SPK_DET.STRUCTURE_FG                                                                STRUCTURE_FLAG,
             CS_SPK_DET.RPT_TO_GV1_FG                                                               GOV_REPORTED_FLAG,
             CS_SPK_DET.RPT_TO_GV3_FG,
             CS_SPK_DET.RPT_TO_GV4_FG,
             CS_SPK_DET.COMBINED_CRS_FG                                                             COMBINED_COURSE,
             TO_TIMESTAMP_NTZ(
                         TO_CHAR(CS_SPK_DET.CRDATEI, 'YYYY-MM-DD') || ' ' || LPAD(CS_SPK_DET.CRTIMEI, 6, '0'),
                         'YYYY-MM-DD HH24MISS')                                                     COURSE_CREATION_DTS,
             TO_TIMESTAMP_NTZ(
                         TO_CHAR(CS_SPK_DET.LAST_MOD_DATEI, 'YYYY-MM-DD') || ' ' ||
                         LPAD(CS_SPK_DET.LAST_MOD_TIMEI, 6, '0'),
                         'YYYY-MM-DD HH24MISS')                                                     COURSE_LAST_MODIFIED_DTS,
             CS_SPK_DET.MAX_FUNDED_EFTSU                                                            MAXIMUM_GOVERNMENT_FUNDED_EFTSU,
             CS_SPK_DET.EXP_EFTSU_TO_COMP                                                           EXPECTED_EFTSU_TO_COMPLETE,
             CS_SPK_DET.EXP_TIME_COMP                                                               EXPECTED_TIME_TO_COMPLETE,
             CS_SPK_DET.MIN_EFTSU_TO_COMP                                                           MINIMUM_EFTSU_TO_COMPLETE,
             CS_SPK_DET.MAX_EFTSU_TO_COMP                                                           MAXIMUM_EFTSU_TO_COMPLETE,
             IFF(CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL = 0, CSP_STUDY_MEASURE_DET.ANNUAL_LOAD_VAL,
                 CS_SPK_STUDY_MEASURE.ANNUAL_LOAD_VAL)                                              COURSE_ANNUAL_CREDIT_POINTS,
             CASE
                 WHEN CS_SPK_DET.SPK_CAT_TYPE_CD in ('110', '130', '178', '122', '203') THEN 'Y'
                 ELSE 'N' END                                                                       HDR_COURSE,
             CAT_TYPE.SPK_CAT_LVL_CD                                                                COURSE_CAT_TYPE_LEVEL,
             CASE WHEN CAT_TYPE.CRS_TYPE_CD IN ('42', '44') THEN 'Y' ELSE 'N' END                   CROSS_INSTITUTION_COURSE,
             OWN_ORG_UNIT.ORG_UNIT_CD                                                               COURSE_OWNING_ORG_UNIT_CODE,
             OWN_ORG_UNIT.ORG_UNIT_TYPE                                                             COURSE_OWNING_ORG_UNIT_TYPE,
             OWN_ORG_UNIT.ORG_UNIT_NM                                                               COURSE_OWNING_ORG_UNIT_NAME,
             CAT_TYPE.CRS_TYPE_CD                                                                   COURSE_TYPE_CODE,
             CRS_TYPE_CODE.CODE_DESCR                                                               COURSE_TYPE_DESCRIPTION,
             REF_LEVEL_OF_STUDY.LEVEL_OF_STUDY,
             FOE.PRIMARY_FIELD_OF_EDUCATION_CODE                                                    PRIMARY_FIELD_OF_EDUCATION_CODE,
             FOE.PRIMARY_FIELD_OF_EDUCATION_DESC                                                    PRIMARY_FIELD_OF_EDUCATION_DESC,
             FOE.SECONDARY_FIELD_OF_EDUCATION_CODE                                                  SECONDARY_FIELD_OF_EDUCATION_CODE,
             FOE.SECONDARY_FIELD_OF_EDUCATION_DESC                                                  SECONDARY_FIELD_OF_EDUCATION_DESC,
             CIRCOS_COURSE.CORRSPND_CD                                                              CRICOS_CODE,
             CASE
                 WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '110' THEN 'PHD'
                 WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '122' THEN 'Combined PHD'
                 WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '178' THEN 'BPhil'
                 WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '130' AND UPPER(CS_SPK_DET.SPK_FULL_TITLE) LIKE 'MASTER OF PHILOSOPHY%'
                     THEN 'MPhil'
                 WHEN CAT_TYPE.SPK_CAT_TYPE_CD = '130' THEN 'Mres'
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
             CS_SPK_DET.CRS_SPEC_CD                                                                 SPECIAL_COURSE_CODE
      FROM ODS.AMIS.S1SPK_DET CS_SPK_DET
               JOIN ODS.AMIS.S1CAT_TYPE CAT_TYPE
                    ON CAT_TYPE.SPK_CAT_TYPE_CD = CS_SPK_DET.SPK_CAT_TYPE_CD
               LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SPK_STAGE_CODE
                               ON SPK_STAGE_CODE.CODE_ID = CS_SPK_DET.SPK_STAGE_CD AND
                                  SPK_STAGE_CODE.CODE_TYPE = 'SPK_STAGE_CD'
               LEFT OUTER JOIN (SELECT A.SPK_NO,
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
                                GROUP BY A.SPK_NO, A.SPK_VER_NO) FOE
                               ON FOE.SPK_NO = CS_SPK_DET.SPK_NO AND FOE.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
               LEFT OUTER JOIN (SELECT CS_SPK_DET.SPK_NO,
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
                                GROUP BY CS_SPK_DET.SPK_NO, CS_SPK_DET.SPK_VER_NO) CIRCOS_COURSE
                               ON CIRCOS_COURSE.SPK_NO = CS_SPK_DET.SPK_NO AND
                                  CIRCOS_COURSE.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
               LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_SIX
                               ON FOE_SIX.CODE = LPAD(COALESCE(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, '0'), 6, '0')
               LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_FOUR
                               ON FOE_FOUR.CODE =
                                  SUBSTR(LPAD(COALESCE(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, '0'), 6, '0'), 1, 4)
               LEFT OUTER JOIN ODS.EXT_REF.FIELD_OF_EDUCATION FOE_TWO
                               ON FOE_TWO.CODE =
                                  SUBSTR(LPAD(COALESCE(FOE.PRIMARY_FIELD_OF_EDUCATION_CODE, '0'), 6, '0'), 1, 2)
               LEFT OUTER JOIN (SELECT SPK_NO, SPK_VER_NO, MAX(ORG_UNIT_CD) ORG_UNIT_CD
                                FROM ODS.AMIS.S1SPK_ORG_UNIT
                                WHERE RESP_CAT_CD = 'O'
                                GROUP BY SPK_NO, SPK_VER_NO) SPK_OWN_ORG_UNIT
                               ON SPK_OWN_ORG_UNIT.SPK_NO = CS_SPK_DET.SPK_NO AND
                                  SPK_OWN_ORG_UNIT.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
               LEFT OUTER JOIN (SELECT A.ORG_UNIT_CD,
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
                                WHERE A.RN = 1) OWN_ORG_UNIT
                               ON OWN_ORG_UNIT.ORG_UNIT_CD = CASE
                                                                 WHEN SUBSTR(SPK_OWN_ORG_UNIT.ORG_UNIT_CD, 1, 1) = '8'
                                                                     THEN '9011'
                                                                 ELSE SUBSTR(SPK_OWN_ORG_UNIT.ORG_UNIT_CD, 1, 1) || '011'
                                   END
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
               LEFT OUTER JOIN ODS.AMIS.S1STC_CODE CRS_TYPE_CODE
                               ON CAT_TYPE.CRS_TYPE_CD = CRS_TYPE_CODE.CODE_ID AND
                                  CRS_TYPE_CODE.CODE_TYPE = 'CRS_TYPE_CD'
               LEFT OUTER JOIN ODS.EXT_REF.REF_LEVEL_OF_STUDY REF_LEVEL_OF_STUDY
                               ON REF_LEVEL_OF_STUDY.COURSE_OF_STUDY_TYPE_CODE = CAT_TYPE.CRS_TYPE_CD
      WHERE CS_SPK_DET.SPK_CAT_CD = 'CS') A
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_COURSE_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_COURSE_AMIS) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_COURSE_KEY = MD5(
                   IFNULL(A.COURSE_CODE, '') || ',' ||
                   IFNULL(A.COURSE_VERSION, 0)
           )
          AND S.HASH_MD5 = MD5(IFNULL(COURSE_SPK_NO, 0) || ',' ||
           IFNULL(COURSE_VERSION, 0) || ',' ||
           IFNULL(COURSE_CODE, '') || ',' ||
           IFNULL(COURSE_SORT_CODE, '') || ',' ||
           IFNULL(COURSE_CAT_TYPE_CODE, '') || ',' ||
           IFNULL(COURSE_STAGE_CODE, '') || ',' ||
           IFNULL(COURSE_STAGE, '') || ',' ||
           IFNULL(COURSE_FULL_TITLE, '') || ',' ||
           IFNULL(COURSE_SHORT_TITLE, '') || ',' ||
           IFNULL(COURSE_ABBREVIATED_TITLE, '') || ',' ||
           IFNULL(EFFECTIVE_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(DISCONTINUED_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STRUCTURE_FLAG, '') || ',' ||
           IFNULL(GOV_REPORTED_FLAG, '') || ',' ||
           IFNULL(RPT_TO_GV3_FG, '') || ',' ||
           IFNULL(RPT_TO_GV4_FG, '') || ',' ||
           IFNULL(COMBINED_COURSE, '') || ',' ||
           IFNULL(COURSE_CREATION_DTS, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(COURSE_LAST_MODIFIED_DTS, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(MAXIMUM_GOVERNMENT_FUNDED_EFTSU, 0) || ',' ||
           IFNULL(EXPECTED_EFTSU_TO_COMPLETE, 0) || ',' ||
           IFNULL(EXPECTED_TIME_TO_COMPLETE, 0) || ',' ||
           IFNULL(MINIMUM_EFTSU_TO_COMPLETE, 0) || ',' ||
           IFNULL(MAXIMUM_EFTSU_TO_COMPLETE, 0) || ',' ||
           IFNULL(COURSE_ANNUAL_CREDIT_POINTS, 0) || ',' ||
           IFNULL(HDR_COURSE, '') || ',' ||
           IFNULL(COURSE_CAT_TYPE_LEVEL, '') || ',' ||
           IFNULL(CROSS_INSTITUTION_COURSE, '') || ',' ||
           IFNULL(COURSE_OWNING_ORG_UNIT_CODE, '') || ',' ||
           IFNULL(COURSE_OWNING_ORG_UNIT_TYPE, '') || ',' ||
           IFNULL(COURSE_OWNING_ORG_UNIT_NAME, '') || ',' ||
           IFNULL(COURSE_TYPE_CODE, '') || ',' ||
           IFNULL(COURSE_TYPE_DESCRIPTION, '') || ',' ||
           IFNULL(LEVEL_OF_STUDY, '') || ',' ||
           IFNULL(PRIMARY_FIELD_OF_EDUCATION_CODE, '') || ',' ||
           IFNULL(PRIMARY_FIELD_OF_EDUCATION_DESC, '') || ',' ||
           IFNULL(SECONDARY_FIELD_OF_EDUCATION_CODE, '') || ',' ||
           IFNULL(SECONDARY_FIELD_OF_EDUCATION_DESC, '') || ',' ||
           IFNULL(CRICOS_CODE, '') || ',' ||
           IFNULL(HDR_PROGRAM, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_6_DIGIT_CODE, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_6_DIGIT_DESCRIPTION, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_4_DIGIT_CODE, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_4_DIGIT_DESCRIPTION, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_2_DIGIT_CODE, '') || ',' ||
           IFNULL(FIELD_OF_EDUCATION_2_DIGIT_DESCRIPTION, '') || ',' ||
           IFNULL(SPECIAL_COURSE_CODE, '') || ',' ||
           IFNULL('N', '')
           )
    )
;
