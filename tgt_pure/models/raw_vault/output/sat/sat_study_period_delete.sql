-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_STUDY_PERIOD (
                                                SAT_STUDY_PERIOD_SK,
                                                HUB_STUDY_PERIOD_KEY,
                                                SOURCE,
                                                LOAD_DTS,
                                                ETL_JOB_ID,
                                                HASH_MD5,
                                                IS_DELETED )

SELECT  DATA_VAULT.CORE.SEQ.NEXTVAL SAT_STUDY_PERIOD_SK,
        S.HUB_STUDY_PERIOD_KEY,
       'AMIS' SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('') HASH_MD5,
       'Y' IS_DELETED
FROM (
         SELECT SAT.HUB_STUDY_PERIOD_KEY,
                SAT.CALDR_YR,
                SAT.LOCATION_CD,
                SAT.SPRD_CD,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS) OVER(PARTITION BY SAT.HUB_STUDY_PERIOD_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_STUDY_PERIOD SAT
                  JOIN DATA_VAULT.CORE.HUB_STUDY_PERIOD HUB
                       ON HUB.HUB_STUDY_PERIOD_KEY = SAT.HUB_STUDY_PERIOD_KEY AND HUB.SOURCE = 'AMIS'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM  ODS.AMIS.S1CYR_LOC_DET CYR_LOC_DET
        WHERE CYR_LOC_DET.CALDR_YR = S.CALDR_YR
        AND   CYR_LOC_DET.LOCATION_CD = S.LOCATION_CD
        AND   CYR_LOC_DET.SPRD_CD = S.SPRD_CD
    )
;