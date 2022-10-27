INSERT INTO DATA_VAULT.CORE.SAT_STRUCTURE_AMIS (SAT_STRUCTURE_AMIS_SK, HUB_STRUCTURE_KEY, SOURCE, LOAD_DTS, ETL_JOB_ID, HASH_MD5,
                                           SPK_CD, SPK_VER_NO, SPK_NO, SPK_CAT_CD, SPK_CAT_TYPE_CD, SPK_STAGE_CD,
                                           SPK_CAT_TYPE, SPK_STAGE, SPK_CAT, YEAR_LVL, SPK_FULL_TITLE, SPK_SHORT_TITLE,
                                           SPK_ABBR_TITLE, EFFCT_DT, DISCNT_DATE, OWNING_ORG_UNIT_CD,
                                           OWNING_ORG_UNIT_NAME, TEACHING_ORG_CODE_1, TEACHING_ORG_UNIT_NAME_1,
                                           TEACHING_PERCENTAGE_1, TEACHING_ORG_CODE_2, TEACHING_ORG_UNIT_NAME_2,
                                           TEACHING_PERCENTAGE_2, TEACHING_ORG_CODE_3, TEACHING_ORG_UNIT_NAME_3,
                                           TEACHING_PERCENTAGE_3, CREDIT_MEASURE_VALUE_$CP, CREDIT_MEASURE_VALUE_CP2,
                                           OWNING_FACULTY_CODE, OWNING_FACULTY_NAME, IS_DELETED)
WITH ORG_UNIT AS (
    SELECT ORG1.ORG_UNIT_CD, ORG1.ORG_UNIT_NM
    FROM
    (SELECT ORG_UNIT_CD,
                         ORG_UNIT_NM,
                         EFFCT_DT,
                         EXPIRY_DT,
                         ROW_NUMBER() OVER (PARTITION BY
                             ORG_UNIT_CD ORDER BY
                             EFFCT_DT DESC) RN
                  FROM ODS.AMIS.S1ORG_UNIT) ORG1
    WHERE ORG1.RN=1
    )
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL                                                            SAT_STRUCTURE_AMIS_SK,
       MD5(
                   IFNULL(STRUC_SPK_DET.SPK_CD, '') || ',' ||
                   IFNULL(STRUC_SPK_DET.SPK_VER_NO, 0) )            HUB_STRUCTURE_KEY,
       'AMIS'                                                                                 SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                                       LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                              ETL_JOB_ID,
       MD5(
           IFNULL(STRUC_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_NO, 0) || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_CAT_CD, '') || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_CAT_TYPE_CD, '') || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_STAGE_CD, '') || ',' ||
           IFNULL(STUDY_PACK_STAGE_TYPE_CODE.SPK_CAT_TYPE_DESC, '') || ',' ||
           IFNULL(STUDY_PACK_STAGE_CODE.CODE_DESCR, '') || ',' ||
           IFNULL(STUDY_PACK_CAT_CODE.CODE_DESCR, '') || ',' ||
           IFNULL(STRUC_SPK_DET.YEAR_LVL, 0) || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_FULL_TITLE, '') || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_SHORT_TITLE, '') || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_ABBR_TITLE, '') || ',' ||
           IFNULL(STRUC_SPK_DET.EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STRUC_SPK_DET.DISCNT_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(OWNING_ORG.OWNING_ORG_UNIT_CD, '') || ',' ||
           IFNULL(OWNING_ORG_UNIT_NM.ORG_UNIT_NM, '') || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_ORG_CODE_1, 0) || ',' ||
           IFNULL(TEACHING_ORG_UNIT_NM_1.ORG_UNIT_NM, '') || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_1, 0) || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_ORG_CODE_2, 0) || ',' ||
           IFNULL(TEACHING_ORG_UNIT_NM_2.ORG_UNIT_NM, '') || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_2, 0) || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_ORG_CODE_3, 0) || ',' ||
           IFNULL(TEACHING_ORG_UNIT_NM_3.ORG_UNIT_NM, '') || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_3, 0) || ',' ||
           IFNULL(STUDY_MEASURE.CREDIT_MEASURE_VALUE_$CP, 0) || ',' ||
           IFNULL(STUDY_MEASURE.CREDIT_MEASURE_VALUE_CP2, 0) || ',' ||
           IFNULL(OWN_ORG_FACULTY.ORG_UNIT_CD, '') || ',' ||
           IFNULL(OWN_ORG_FACULTY.ORG_UNIT_NM, '') || ',' ||
           IFNULL('N', '')
           )                                                                                  HASH_MD5,
       STRUC_SPK_DET.SPK_CD,
       STRUC_SPK_DET.SPK_VER_NO,
       STRUC_SPK_DET.SPK_NO,
       STRUC_SPK_DET.SPK_CAT_CD,
       STRUC_SPK_DET.SPK_CAT_TYPE_CD,
       STRUC_SPK_DET.SPK_STAGE_CD,
       STUDY_PACK_STAGE_TYPE_CODE.SPK_CAT_TYPE_DESC                                           SPK_CAT_TYPE,
       STUDY_PACK_STAGE_CODE.CODE_DESCR                                                       SPK_STAGE,
       STUDY_PACK_CAT_CODE.CODE_DESCR                                                         SPK_CAT,
       STRUC_SPK_DET.YEAR_LVL,
       STRUC_SPK_DET.SPK_FULL_TITLE,
       STRUC_SPK_DET.SPK_SHORT_TITLE,
       STRUC_SPK_DET.SPK_ABBR_TITLE,
       STRUC_SPK_DET.EFFCT_DT,
       STRUC_SPK_DET.DISCNT_DATE,
       OWNING_ORG.OWNING_ORG_UNIT_CD,
       OWNING_ORG_UNIT_NM.ORG_UNIT_NM                                                         OWNING_ORG_UNIT_NAME,
       TEACHING_ORG_UNIT.TEACHING_ORG_CODE_1,
       TEACHING_ORG_UNIT_NM_1.ORG_UNIT_NM                                                     TEACHING_ORG_UNIT_NAME_1,
       TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_1,
       TEACHING_ORG_UNIT.TEACHING_ORG_CODE_2,
       TEACHING_ORG_UNIT_NM_2.ORG_UNIT_NM                                                     TEACHING_ORG_UNIT_NAME_2,
       TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_2,
       TEACHING_ORG_UNIT.TEACHING_ORG_CODE_3,
       TEACHING_ORG_UNIT_NM_3.ORG_UNIT_NM                                                     TEACHING_ORG_UNIT_NAME_3,
       TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_3,
       STUDY_MEASURE.CREDIT_MEASURE_VALUE_$CP,
       STUDY_MEASURE.CREDIT_MEASURE_VALUE_CP2,
       OWN_ORG_FACULTY.ORG_UNIT_CD                                                            OWNING_FACULTY_CODE,
       OWN_ORG_FACULTY.ORG_UNIT_NM                                                            OWNING_FACULTY_NAME,
       'N'                                                                                    IS_DELETED
FROM ODS.AMIS.S1SPK_DET STRUC_SPK_DET
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_PACK_CAT_CODE
                         ON STUDY_PACK_CAT_CODE.CODE_ID = STRUC_SPK_DET.SPK_CAT_CD
                             AND STUDY_PACK_CAT_CODE.CODE_TYPE = 'SPK_CAT_CD'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE STUDY_PACK_STAGE_CODE
                         ON STUDY_PACK_STAGE_CODE.CODE_ID = STRUC_SPK_DET.SPK_STAGE_CD
                             AND STUDY_PACK_STAGE_CODE.CODE_TYPE = 'SPK_STAGE_CD'
         LEFT OUTER JOIN ODS.AMIS.S1CAT_TYPE STUDY_PACK_STAGE_TYPE_CODE
                         ON STUDY_PACK_STAGE_TYPE_CODE.SPK_CAT_CD = STRUC_SPK_DET.SPK_CAT_CD
                             AND STUDY_PACK_STAGE_TYPE_CODE.SPK_CAT_TYPE_CD = STRUC_SPK_DET.SPK_CAT_TYPE_CD
         LEFT OUTER JOIN (SELECT SPK_NO, SPK_VER_NO, MAX(ORG_UNIT_CD) OWNING_ORG_UNIT_CD
                          FROM ODS.AMIS.S1SPK_ORG_UNIT OWNING_ORG_UNIT
                          WHERE RESP_CAT_CD = 'O'
                          GROUP BY SPK_NO, SPK_VER_NO) OWNING_ORG
                         ON OWNING_ORG.SPK_NO = STRUC_SPK_DET.SPK_NO AND OWNING_ORG.SPK_VER_NO = STRUC_SPK_DET.SPK_VER_NO
         LEFT OUTER JOIN (SELECT A.SPK_NO,
                                 A.SPK_VER_NO,
                                 MAX(IFF(A.RN = 1, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_1,
                                 MAX(IFF(A.RN = 1, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_1,
                                 MAX(IFF(A.RN = 2, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_2,
                                 MAX(IFF(A.RN = 2, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_2,
                                 MAX(IFF(A.RN = 3, A.ORG_UNIT_CD, NULL)) TEACHING_ORG_CODE_3,
                                 MAX(IFF(A.RN = 3, A.PCENT_RESP, NULL))  TEACHING_PERCENTAGE_3
                          FROM (SELECT SPK_NO,
                                       SPK_VER_NO,
                                       ORG_UNIT_CD,
                                       PCENT_RESP,
                                       ROW_NUMBER()
                                               OVER (PARTITION BY SPK_NO, SPK_VER_NO ORDER BY ORG_UNIT_RESP_NO) RN
                                FROM ODS.AMIS.S1SPK_ORG_UNIT OWNING_ORG_UNIT
                                WHERE RESP_CAT_CD = 'T') A
                          GROUP BY A.SPK_NO, A.SPK_VER_NO) TEACHING_ORG_UNIT
                         ON TEACHING_ORG_UNIT.SPK_NO = STRUC_SPK_DET.SPK_NO AND
                            TEACHING_ORG_UNIT.SPK_VER_NO = STRUC_SPK_DET.SPK_VER_NO
         LEFT OUTER JOIN ORG_UNIT OWNING_ORG_UNIT_NM
                         ON OWNING_ORG_UNIT_NM.ORG_UNIT_CD = OWNING_ORG.OWNING_ORG_UNIT_CD
         LEFT OUTER JOIN ORG_UNIT TEACHING_ORG_UNIT_NM_1
                         ON TEACHING_ORG_UNIT_NM_1.ORG_UNIT_CD = TEACHING_ORG_UNIT.TEACHING_ORG_CODE_1
         LEFT OUTER JOIN ORG_UNIT TEACHING_ORG_UNIT_NM_2
                         ON TEACHING_ORG_UNIT_NM_2.ORG_UNIT_CD = TEACHING_ORG_UNIT.TEACHING_ORG_CODE_2
         LEFT OUTER JOIN ORG_UNIT TEACHING_ORG_UNIT_NM_3
                         ON TEACHING_ORG_UNIT_NM_3.ORG_UNIT_CD = TEACHING_ORG_UNIT.TEACHING_ORG_CODE_3
         LEFT OUTER JOIN (SELECT SPK_NO,
                                 SPK_VER_NO,
                                 MAX(IFF(STUDY_MEASURE_CD = '$CP', STUDY_MEASURE_VAL, NULL)) CREDIT_MEASURE_VALUE_$CP,
                                 MAX(IFF(STUDY_MEASURE_CD = 'CP2', STUDY_MEASURE_VAL, NULL)) CREDIT_MEASURE_VALUE_CP2
                          FROM ODS.AMIS.S1SPK_STUDY_MEASURE
                          GROUP BY SPK_NO, SPK_VER_NO) STUDY_MEASURE
                         ON STUDY_MEASURE.SPK_NO = STRUC_SPK_DET.SPK_NO AND
                            STUDY_MEASURE.SPK_VER_NO = STRUC_SPK_DET.SPK_VER_NO
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
                          WHERE A.RN = 1) OWN_ORG_FACULTY
                         ON OWN_ORG_FACULTY.ORG_UNIT_CD = CASE
                                                              WHEN SUBSTR(OWNING_ORG.OWNING_ORG_UNIT_CD, 1, 1) = '8'
                                                                  THEN '9011'
                                                              ELSE SUBSTR(OWNING_ORG.OWNING_ORG_UNIT_CD, 1, 1) || '011'
                             END
WHERE STRUC_SPK_DET.SPK_CAT_CD IN (
                     'SS',
                     'MJ',
                     'SP',
                     'MN',
                     'ST'
    )
  AND NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_STRUCTURE_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_STRUCTURE_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_STRUCTURE_AMIS) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_STRUCTURE_KEY = MD5(
                    IFNULL(STRUC_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(STRUC_SPK_DET.SPK_VER_NO, 0)
              )
          AND S.HASH_MD5 = MD5(
              IFNULL(STRUC_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_NO, 0) || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_CAT_CD, '') || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_CAT_TYPE_CD, '') || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_STAGE_CD, '') || ',' ||
           IFNULL(STUDY_PACK_STAGE_TYPE_CODE.SPK_CAT_TYPE_DESC, '') || ',' ||
           IFNULL(STUDY_PACK_STAGE_CODE.CODE_DESCR, '') || ',' ||
           IFNULL(STUDY_PACK_CAT_CODE.CODE_DESCR, '') || ',' ||
           IFNULL(STRUC_SPK_DET.YEAR_LVL, 0) || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_FULL_TITLE, '') || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_SHORT_TITLE, '') || ',' ||
           IFNULL(STRUC_SPK_DET.SPK_ABBR_TITLE, '') || ',' ||
           IFNULL(STRUC_SPK_DET.EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STRUC_SPK_DET.DISCNT_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(OWNING_ORG.OWNING_ORG_UNIT_CD, '') || ',' ||
           IFNULL(OWNING_ORG_UNIT_NM.ORG_UNIT_NM, '') || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_ORG_CODE_1, 0) || ',' ||
           IFNULL(TEACHING_ORG_UNIT_NM_1.ORG_UNIT_NM, '') || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_1, 0) || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_ORG_CODE_2, 0) || ',' ||
           IFNULL(TEACHING_ORG_UNIT_NM_2.ORG_UNIT_NM, '') || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_2, 0) || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_ORG_CODE_3, 0) || ',' ||
           IFNULL(TEACHING_ORG_UNIT_NM_3.ORG_UNIT_NM, '') || ',' ||
           IFNULL(TEACHING_ORG_UNIT.TEACHING_PERCENTAGE_3, 0) || ',' ||
           IFNULL(STUDY_MEASURE.CREDIT_MEASURE_VALUE_$CP, 0) || ',' ||
           IFNULL(STUDY_MEASURE.CREDIT_MEASURE_VALUE_CP2, 0) || ',' ||
           IFNULL(OWN_ORG_FACULTY.ORG_UNIT_CD, '') || ',' ||
           IFNULL(OWN_ORG_FACULTY.ORG_UNIT_NM, '') || ',' ||
           IFNULL('N', '')
            )
    )
;
