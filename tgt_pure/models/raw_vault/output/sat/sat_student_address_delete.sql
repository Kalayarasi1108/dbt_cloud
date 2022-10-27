-- DELETE
INSERT INTO CORE.SAT_STUDENT_ADDRESS
(SAT_STUDENT_ADDRESS_SK,
 HUB_STUDENT_KEY,
 SOURCE,
 LOAD_DTS,
 ETL_JOB_ID,
 HASH_MD5,
 HOME_ADDRESS_LINE_1,
 HOME_ADDRESS_LINE_2,
 HOME_ADDRESS_LINE_3,
 HOME_SUBURB,
 HOME_STATE,
 HOME_POST_CODE,
 HOME_COUNTRY,
 HOME_PREF_MAIL_FG,
 CONTACT_ADDRESS_LINE_1,
 CONTACT_ADDRESS_LINE_2,
 CONTACT_ADDRESS_LINE_3,
 CONTACT_SUBURB,
 CONTACT_STATE,
 CONTACT_POST_CODE,
 CONTACT_COUNTRY,
 CONTACT_PREF_MAIL_FG,
 TEMPORARY_ADDRESS_LINE_1,
 TEMPORARY_ADDRESS_LINE_2,
 TEMPORARY_ADDRESS_LINE_3,
 TEMPORARY_SUBURB,
 TEMPORARY_STATE,
 TEMPORARY_POST_CODE,
 TEMPORARY_COUNTRY,
 TEMPORARY_PREF_MAIL_FG,
 IS_DELETED)
SELECT CORE.SEQ.NEXTVAL                          SAT_STUDENT_ADDRESS_SK,
       S.HUB_STUDENT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                  HASH_MD5,
       NULL                                        HOME_ADDRESS_LINE_1,
       NULL                                        HOME_ADDRESS_LINE_2,
       NULL                                        HOME_ADDRESS_LINE_3,
       NULL                                        HOME_SUBURB,
       NULL                                        HOME_STATE,
       NULL                                        HOME_POST_CODE,
       NULL                                        HOME_COUNTRY,
       NULL                                        HOME_PREF_MAIL_FG,
       NULL                                        CONTACT_ADDRESS_LINE_1,
       NULL                                        CONTACT_ADDRESS_LINE_2,
       NULL                                        CONTACT_ADDRESS_LINE_3,
       NULL                                        CONTACT_SUBURB,
       NULL                                        CONTACT_STATE,
       NULL                                        CONTACT_POST_CODE,
       NULL                                        CONTACT_COUNTRY,
       NULL                                        CONTACT_PREF_MAIL_FG,
       NULL                                        TEMPORARY_ADDRESS_LINE_1,
       NULL                                        TEMPORARY_ADDRESS_LINE_2,
       NULL                                        TEMPORARY_ADDRESS_LINE_3,
       NULL                                        TEMPORARY_SUBURB,
       NULL                                        TEMPORARY_STATE,
       NULL                                        TEMPORARY_POST_CODE,
       NULL                                        TEMPORARY_COUNTRY,
       NULL                                        TEMPORARY_PREF_MAIL_FG,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB.STU_ID,
                SAT.HUB_STUDENT_KEY,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS) OVER (PARTITION BY SAT.HUB_STUDENT_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_STUDENT_ADDRESS SAT
                  JOIN DATA_VAULT.CORE.HUB_STUDENT HUB
                       ON HUB.HUB_STUDENT_KEY = SAT.HUB_STUDENT_KEY AND HUB.SOURCE = 'AMIS'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1STU_DET STU_DET
        WHERE STU_DET.STU_ID = S.STU_ID
    )
;
