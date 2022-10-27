INSERT INTO DATA_VAULT.CORE.SAT_HDR_SCHOLARSHIP (SAT_HDR_SCHOLARSHIP_SK, HUB_HDR_SCHOLARSHIP_KEY, SOURCE,
                                                 LOAD_DTS,
                                                 ETL_JOB_ID, HASH_MD5,
                                                 IS_DELETED)
select DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_HDR_SCHOLARSHIP_SK,
       S.HUB_HDR_SCHOLARSHIP_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
from (
         SELECT HUB.STU_ID,
                SAT.HUB_HDR_SCHOLARSHIP_KEY,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_HDR_SCHOLARSHIP_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_HDR_SCHOLARSHIP SAT
                  JOIN DATA_VAULT.CORE.HUB_HDR_SCHOLARSHIP HUB
                       ON HUB.HUB_HDR_SCHOLARSHIP_KEY = SAT.HUB_HDR_SCHOLARSHIP_KEY AND HUB.SOURCE = 'AMIS'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
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
        WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
          AND CS_SSP.SSP_STG_CD IN ('ADM', 'COMP')
          AND CS_SSP.LIAB_CAT_CD NOT IN ('NX')
          AND S.HUB_HDR_SCHOLARSHIP_KEY = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                                              IFNULL(CS_SPK.SPK_CD, '') || ',' ||
                                              IFNULL(CS_SPK.SPK_NO, 0) || ',' ||
                                              IFNULL(CS_SSP.PARENT_SSP_NO, 0) || ',' ||
                                              IFNULL(SCHOL_COMM.AVAIL_YR, 0) || ',' ||
                                              IFNULL(SCHOL_COMM.CMT_CD, '') || ','
            )
    )
;