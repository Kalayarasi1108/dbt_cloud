INSERT INTO DATA_VAULT.CORE.SAT_COURSE_OFFERING_AMIS (SAT_COURSE_OFFERING_AMIS_SK, HUB_COURSE_OFFERING_KEY, SOURCE,
                                                      LOAD_DTS,
                                                      ETL_JOB_ID, HASH_MD5, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_OFFERING_AMIS_SK,
       S.HUB_COURSE_OFFERING_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB_COURSE_OFFERING_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_OFFERING_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_COURSE_OFFERING_AMIS
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SPK_DET CS_SPK_DET
                 JOIN ODS.AMIS.S1SPK_AVAIL_DET CS_AVAIL_DET
                      ON CS_AVAIL_DET.SPK_NO = CS_SPK_DET.SPK_NO
                          AND CS_AVAIL_DET.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
        WHERE CS_SPK_DET.SPK_CAT_CD = 'CS'
          AND S.HUB_COURSE_OFFERING_KEY = MD5(
                    IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                    IFNULL(CS_AVAIL_DET.AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(CS_AVAIL_DET.AVAIL_YR, 0) || ',' ||
                    IFNULL(CS_AVAIL_DET.SPRD_CD, '') || ',' ||
                    IFNULL(CS_AVAIL_DET.LOCATION_CD, '')
            )
    )
;