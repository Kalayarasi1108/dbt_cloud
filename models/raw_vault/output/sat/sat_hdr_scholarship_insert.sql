INSERT INTO DATA_VAULT.CORE.SAT_HDR_SCHOLARSHIP (SAT_HDR_SCHOLARSHIP_SK, HUB_HDR_SCHOLARSHIP_KEY, SOURCE, LOAD_DTS,
                                                 ETL_JOB_ID, HASH_MD5,
                                                 STU_ID, COURSE_SPK_CD, COURSE_SPK_NO, PARENT_SSP_NO,
                                                 CS_STG_CD, CS_STTS_CD, SCHOLARSHIP_YEAR, SCHOLARSHIP_CODE,
                                                 STU_CMT_EFFCT_DT, STU_CMT_TEXT, CMT_DESCRIPTION,
                                                 SCHOLARSHIP_DESCRIPTION, SCHOLARSHIP_TYPE,
                                                 COTUTELLE_OR_NOT_COTUTELLE, IS_DELETED)
select DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_HDR_SCHOLARSHIP_SK,
       MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
           IFNULL(CS_SPK.SPK_CD, '') || ',' ||
           IFNULL(CS_SPK.SPK_NO, 0) || ',' ||
           IFNULL(CS_SSP.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(SCHOL_COMM.AVAIL_YR, 0) || ',' ||
           IFNULL(SCHOL_COMM.CMT_CD, '') || ','
           )                                     HUB_HDR_SCHOLARSHIP_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
           IFNULL(CS_SPK.SPK_CD, '') || ',' ||
           IFNULL(CS_SSP.SPK_NO, 0) || ',' ||
           IFNULL(CS_SSP.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_STG_CD, '') || ',' ||
           IFNULL(CS_SSP.SSP_STTS_CD, '') || ',' ||
           IFNULL(SCHOL_COMM.AVAIL_YR, 0) || ',' ||
           IFNULL(SCHOL_COMM.CMT_CD, '') || ',' ||
           IFNULL(SCHOL_COMM.STU_CMT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SCHOL_COMM.STU_CMT_TXT_1, '') || ',' ||
           IFNULL(SCHOL_COMM.CMT_DESC, '') || ',' ||
           IFNULL(SCHOL_COMM.CMT_FIXED_TXT, '') || ',' ||
           IFNULL(SCHOL.SCHOLARSHIP_TYPE, '') || ',' ||
           IFNULL(SCHOL.COTUTELLE_OR_NOT_COTUTELLE, '') || ',' ||
           IFNULL('N', '')
           )                                     HASH_MD5,
       CS_SSP.STU_ID,
       CS_SPK.SPK_CD                             COURSE_SPK_CD,
       CS_SSP.SPK_NO                             COURSE_SPK_NO,
       CS_SSP.PARENT_SSP_NO,
       CS_SSP.SSP_STG_CD                         CS_STG_CD,
       CS_SSP.SSP_STTS_CD                        CS_STTS_CD,
       SCHOL_COMM.AVAIL_YR                       SCHOLARSHIP_YEAR,
       SCHOL_COMM.CMT_CD                         SCHOLARSHIP_CODE,
       SCHOL_COMM.STU_CMT_EFFCT_DT,
       SCHOL_COMM.STU_CMT_TXT_1                  STU_CMT_TEXT,
       SCHOL_COMM.CMT_DESC                       CMT_DESCRIPTION,
       SCHOL_COMM.CMT_FIXED_TXT                  SCHOLARSHIP_DESCRIPTION,
       SCHOL.SCHOLARSHIP_TYPE                    SCHOLARSHIP_TYPE,
       SCHOL.COTUTELLE_OR_NOT_COTUTELLE          COTUTELLE_OR_NOT_COTUTELLE,
       'N'                                       IS_DELETED
from ODS.AMIS.S1SSP_STU_SPK CS_SSP
         JOIN (SELECT SSP_NO,
                      COALESCE(MAX(DECODE(SSP_DT_TYPE_CD, 'COS', EXPECTED_DATE)),
                               MAX(DECODE(SSP_DT_TYPE_CD, 'STRT', EXPECTED_DATE))) COURSE_START_DATE
               FROM ODS.AMIS.S1SSP_DATE DT
               WHERE DT.SSP_DT_TYPE_CD IN ('COS', 'STRT')
               GROUP BY SSP_NO
) COURSE_START
              ON COURSE_START.SSP_NO = CS_SSP.SSP_NO
         JOIN ODS.AMIS.S1SPK_DET CS_SPK
              ON CS_SPK.SPK_NO = CS_SSP.SPK_NO AND CS_SPK.SPK_VER_NO = CS_SSP.SPK_VER_NO
                  AND CS_SPK.SPK_CAT_TYPE_CD in ('110', '130', '178', '122', '203')
         JOIN (
    SELECT COMM.STU_ID,
           COMM.SPK_NO,
           COMM.SPK_VER_NO,
           COMM.AVAIL_YR,
           COMM.CMT_CD,
           COMM.STU_CMT_EFFCT_DT,
           COMM.STU_CMT_TXT_1,
           CMT_DET.CMT_DESC,
           CMT_DET.CMT_FIXED_TXT,
           ROW_NUMBER() OVER (PARTITION BY COMM.STU_ID, COMM.SPK_NO, COMM.SPK_VER_NO,
               COMM.AVAIL_YR, COMM.CMT_CD ORDER BY COMM.STU_CMT_EFFCT_DT) RN
    FROM ODS.AMIS.S1STU_COMMENT COMM
             JOIN ODS.AMIS.S1CMT_DET CMT_DET
                  ON CMT_DET.CMT_CD = COMM.CMT_CD AND CMT_DET.CMT_TYPE_CD = 'SCHPG'
) SCHOL_COMM
              ON SCHOL_COMM.STU_ID = CS_SSP.STU_ID AND SCHOL_COMM.SPK_NO = CS_SSP.SPK_NO AND
                 SCHOL_COMM.SPK_VER_NO = CS_SSP.SPK_VER_NO
                  AND SCHOL_COMM.AVAIL_YR <= CASE
                                                 WHEN CS_SSP.SSP_STG_CD = 'COMP'
                                                     THEN YEAR(CS_SSP.EFFCT_START_DT)
                                                 ELSE YEAR(CURRENT_DATE) + 2 END
                  AND SCHOL_COMM.AVAIL_YR >= YEAR(COURSE_START.COURSE_START_DATE)
                  AND SCHOL_COMM.RN = 1
         LEFT OUTER JOIN ODS.EXT_REF.REF_HDR_SCHOLARSHIP_CODE SCHOL
                         on SCHOL_COMM.CMT_CD = SCHOL.SCHOLARSHIP_CODE
WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
  AND CS_SSP.SSP_STG_CD IN ('ADM', 'COMP')
  AND CS_SSP.LIAB_CAT_CD NOT IN ('NX')
  AND NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_HDR_SCHOLARSHIP_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_HDR_SCHOLARSHIP_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_HDR_SCHOLARSHIP) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_HDR_SCHOLARSHIP_KEY = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                                              IFNULL(CS_SPK.SPK_CD, '') || ',' ||
                                              IFNULL(CS_SPK.SPK_NO, 0) || ',' ||
                                              IFNULL(CS_SSP.PARENT_SSP_NO, 0) || ',' ||
                                              IFNULL(SCHOL_COMM.AVAIL_YR, 0) || ',' ||
                                              IFNULL(SCHOL_COMM.CMT_CD, '') || ','
            )
          AND S.HASH_MD5 = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                               IFNULL(CS_SPK.SPK_CD, '') || ',' ||
                               IFNULL(CS_SSP.SPK_NO, 0) || ',' ||
                               IFNULL(CS_SSP.PARENT_SSP_NO, 0) || ',' ||
                               IFNULL(CS_SSP.SSP_STG_CD, '') || ',' ||
                               IFNULL(CS_SSP.SSP_STTS_CD, '') || ',' ||
                               IFNULL(SCHOL_COMM.AVAIL_YR, 0) || ',' ||
                               IFNULL(SCHOL_COMM.CMT_CD, '') || ',' ||
                               IFNULL(SCHOL_COMM.STU_CMT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SCHOL_COMM.STU_CMT_TXT_1, '') || ',' ||
                               IFNULL(SCHOL_COMM.CMT_DESC, '') || ',' ||
                               IFNULL(SCHOL_COMM.CMT_FIXED_TXT, '') || ',' ||
                               IFNULL(SCHOL.SCHOLARSHIP_TYPE, '') || ',' ||
                               IFNULL(SCHOL.COTUTELLE_OR_NOT_COTUTELLE, '') || ',' ||
                               IFNULL('N', '')
            )
    )
;