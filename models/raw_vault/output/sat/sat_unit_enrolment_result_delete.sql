INSERT INTO DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_RESULT (SAT_UNIT_ENROLMENT_RESULT_SK,
                                                       HUB_UNIT_ENROLMENT_RESULT_KEY,
                                                       SOURCE,
                                                       LOAD_DTS,
                                                       ETL_JOB_ID,
                                                       HASH_MD5,
                                                       IS_DELETED)

SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_UNIT_ENROLMENT_RESULT_SK,
       S.HUB_UNIT_ENROLMENT_RESULT_KEY           HUB_UNIT_ENROLMENT_RESULT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       DELETED
FROM (
         SELECT SAT.SSP_NO,
                SAT.SEQ_NO,
                HUB.HUB_UNIT_ENROLMENT_RESULT_KEY,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_UNIT_ENROLMENT_RESULT_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_RESULT SAT
                  JOIN DATA_VAULT.CORE.HUB_UNIT_ENROLMENT_RESULT HUB
                       ON SAT.HUB_UNIT_ENROLMENT_RESULT_KEY = HUB.HUB_UNIT_ENROLMENT_RESULT_KEY
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM (
                 SELECT SSP_RSLT_HIST.SSP_NO,
                        SSP_RSLT_HIST.SEQ_NO
                 FROM ODS.AMIS.S1SSP_RSLT_HIST SSP_RSLT_HIST
                          JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
                               ON UN_SPK_DET.SPK_NO = SSP_RSLT_HIST.SPK_NO
                                   AND UN_SPK_DET.SPK_VER_NO = SSP_RSLT_HIST.SPK_VER_NO
             ) UNIT_RESULT
        WHERE S.SSP_NO = UNIT_RESULT.SSP_NO
          AND S.SEQ_NO = UNIT_RESULT.SEQ_NO
    )
;
