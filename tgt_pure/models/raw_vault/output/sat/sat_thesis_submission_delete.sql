-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_THESIS_SUBMISSION (SAT_THESIS_SUBMISSION_SK,
                                                   HUB_THESIS_SUBMISSION_KEY,
                                                   SOURCE,
                                                   LOAD_DTS,
                                                   ETL_JOB_ID,
                                                   HASH_MD5,
                                                   IS_DELETED)

SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_THESIS_SUBMISSION_SK,
       S.HUB_THESIS_SUBMISSION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM (
         SELECT SAT.HUB_THESIS_SUBMISSION_KEY,
                SAT.STU_ID,
                SAT.SSP_NO,
                SAT.SSP_WRK_SUBM_NO,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_THESIS_SUBMISSION_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_THESIS_SUBMISSION SAT
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SSP_WRKSUBM_DTL STU_SPK_WS
                 INNER JOIN ODS.AMIS.S1SSP_STU_SPK STU_SPK
                            ON STU_SPK_WS.SSP_NO = STU_SPK.SSP_NO
        WHERE STU_SPK.STU_ID= S.STU_ID
          AND STU_SPK_WS.SSP_NO = S.SSP_NO
          AND STU_SPK_WS.SSP_WRK_SUBM_NO = S.SSP_WRK_SUBM_NO
    )
;