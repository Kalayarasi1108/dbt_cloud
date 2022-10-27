-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_UAC_APPLICANT_SUM (SAT_UAC_APPLICANT_SUM_SK, HUB_UAC_APPLICANT_KEY, SOURCE, LOAD_DTS,
                                                   ETL_JOB_ID, HASH_MD5, ATAR, SCHOOL_LEAVER, CURRENT_SCHOOL_LEAVER,
                                                   RECENT_SCHOOL_LEAVER, RECENT_SCHOOL_LEAVER_HEIMS, YEAR_LEFT_SCHOOL,
                                                   DATE_OF_BIRTH, QUALIFICATION, CURRENT_ROUND_MQ_PREFERENCE_SUMMARY,
                                                   CURRENT_ROUND_TOTAL_PREFERENCE_SUMMARY,
                                                   CURRENT_ROUND_MQ_HIGHEST_PREFERENCE_NUMBER, MQ_OFFER_SUMMARY,
                                                   TOTAL_OFFER_SUMMARY, GENDER, CURRENT_APPLICATION, SCHOOL_CODE,
                                                   YEAR_OF_APPLICATION, APPLICANT_REFERENCE_NUMBER, GIVEN_NAME, SURNAME,
                                                   HOME_ADDRESS, HOME_SUBURB, HOME_STATE, HOME_POST_CODE,
                                                   PERMANENT_STATE, PERMANENT_POST_CODE, PERMANENT_ADDRESS,
                                                   PERMANENT_SUBURB, EMAIL, PHONE, STATUS_TYPE, CITIZENSHIP_CODE,
                                                   CITIZENSHIP, NATIONAL_LANGUAGE_CODE, NATIONAL_LANGUAGE,
                                                   BASE_RANK_SET_VALUE, HIGHEST_QUALIFICATION_CODE, CHESSN,
                                                   ENTITLEMENT_CONFIRMATION_DATE, RECEIVE_DATE, DATA_ENTRY_DATE,
                                                   COUNTRY_ORIGIN_CODE, COUNTRY_ORIGIN, SCHOOL_NAME, SCHOOL_VET_FLAG,
                                                   SCHOOL_OFFSHORE, SCHOOL_COUNTRY, SCHOOL_POSTAL_CODE, SCHOOL_STATE,
                                                   SCHOOL_ADDRESS, SCHOOL_SUBURB, TITLE, APPLICATION_STATUS, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL                          SAT_UAC_APPLICANT_SK,
       S.HUB_UAC_APPLICANT_KEY,
       S.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       NULL                                      ATAR,
       NULL                                      SCHOOL_LEAVER,
       NULL                                      CURRENT_SCHOOL_LEAVER,
       NULL                                      RECENT_SCHOOL_LEAVER,
       NULL                                      RECENT_SCHOOL_LEAVER_HEIMS,
       NULL                                      YEAR_LEFT_SCHOOL,
       NULL                                      DATE_OF_BIRTH,
       NULL                                      QUALIFICATION,
       NULL                                      CURRENT_ROUND_MQ_PREFERENCE_SUMMARY,
       NULL                                      CURRENT_ROUND_TOTAL_PREFERENCE_SUMMARY,
       NULL                                      CURRENT_ROUND_MQ_HIGHEST_PREFERENCE_NUMBER,
       NULL                                      MQ_OFFER_SUMMARY,
       NULL                                      TOTAL_OFFER_SUMMARY,
       NULL                                      GENDER,
       NULL                                      CURRENT_APPLICATION,
       NULL                                      SCHOOL_CODE,
       NULL                                      YEAR_OF_APPLICATION,
       NULL                                      APPLICANT_REFERENCE_NUMBER,
       NULL                                      GIVEN_NAME,
       NULL                                      SURNAME,
       NULL                                      HOME_ADDRESS,
       NULL                                      HOME_SUBURB,
       NULL                                      HOME_STATE,
       NULL                                      HOME_POST_CODE,
       NULL                                      PERMANENT_STATE,
       NULL                                      PERMANENT_POST_CODE,
       NULL                                      PERMANENT_ADDRESS,
       NULL                                      PERMANENT_SUBURB,
       NULL                                      EMAIL,
       NULL                                      PHONE,
       NULL                                      STATUS_TYPE,
       NULL                                      CITIZENSHIP_CODE,
       NULL                                      CITIZENSHIP,
       NULL                                      NATIONAL_LANGUAGE_CODE,
       NULL                                      NATIONAL_LANGUAGE,
       NULL                                      BASE_RANK_SET_VALUE,
       NULL                                      HIGHEST_QUALIFICATION_CODE,
       NULL                                      CHESSN,
       NULL                                      ENTITLEMENT_CONFIRMATION_DATE,
       NULL                                      RECEIVE_DATE,
       NULL                                      DATA_ENTRY_DATE,
       NULL                                      COUNTRY_ORIGIN_CODE,
       NULL                                      COUNTRY_ORIGIN,
       NULL                                      SCHOOL_NAME,
       NULL                                      SCHOOL_VET_FLAG,
       NULL                                      SCHOOL_OFFSHORE,
       NULL                                      SCHOOL_COUNTRY,
       NULL                                      SCHOOL_POSTAL_CODE,
       NULL                                      SCHOOL_STATE,
       NULL                                      SCHOOL_ADDRESS,
       NULL                                      SCHOOL_SUBURB,
       NULL                                      TITLE,
       NULL                                      APPLICATION_STATUS,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB.REFNUM,
                HUB.SOURCE,
                HUB.YEAR,
                SAT.HUB_UAC_APPLICANT_KEY,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_UAC_APPLICANT_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_UAC_APPLICANT_SUM SAT
                  JOIN DATA_VAULT.CORE.HUB_UAC_APPLICANT HUB
                       ON HUB.HUB_UAC_APPLICANT_KEY = SAT.HUB_UAC_APPLICANT_KEY
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.UAC.VW_ALL_APPLIC APPLIC
        WHERE APPLIC.REFNUM = S.REFNUM
          AND APPLIC.SOURCE = S.SOURCE
          AND APPLIC.YEAR = S.YEAR
    )
;