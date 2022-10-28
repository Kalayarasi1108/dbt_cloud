INSERT INTO DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_SANCTION(SAT_UNIT_ENROLMENT_SANCTION_SK,
                                                          HUB_UNIT_ENROLMENT_SANCTION_KEY, SOURCE, LOAD_DTS,
                                                          ETL_JOB_ID, HASH_MD5, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_ADMISSION_SANCTION_SK,
       s.HUB_UNIT_ENROLMENT_SANCTION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS       LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                          AS       HASH_MD5,
       'Y'                              AS       IS_DELETED

FROM (SELECT HUB_UNIT_ENROLMENT_SANCTION_KEY,
             HASH_MD5,
             LOAD_DTS,
             IS_DELETED,
             LEAD(LOAD_DTS)
                  OVER (PARTITION BY HUB_UNIT_ENROLMENT_SANCTION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
      FROM DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_SANCTION
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SSP_SCT_DTL SCT_DTL
        WHERE S.HUB_UNIT_ENROLMENT_SANCTION_KEY = MD5(IFNULL(SCT_DTL.STU_ID, '') || ',' ||
                                                        IFNULL(SCT_DTL.SEQ_NO, 0)
            )
    )
;