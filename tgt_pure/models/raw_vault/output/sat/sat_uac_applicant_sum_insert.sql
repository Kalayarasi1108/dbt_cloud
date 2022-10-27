-- INSERT
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
                                                   SCHOOL_ADDRESS, SCHOOL_SUBURB, TITLE, APPLICATION_STATUS,
                                                   SCHOOL_MESH_BLOCK, SCHOOL_LATITUDE, SCHOOL_LONGITUDE,
                                                   ALTERNATE_PHONE,
                                                   IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.nextval SAT_UAC_APPLICANT_SUM_SK,
       S.HUB_UAC_APPLICANT_KEY,
       S.SOURCE,
       S.LOAD_DTS,
       S.ETL_JOB_ID,
       MD5(
                   IFNULL(S.ATAR, 0) || ',' ||
                   IFNULL(S.SCHOOL_LEAVER, '') || ',' ||
                   IFNULL(S.CURRENT_SCHOOL_LEAVER, '') || ',' ||
                   IFNULL(S.RECENT_SCHOOL_LEAVER, '') || ',' ||
                   IFNULL(S.RECENT_SCHOOL_LEAVER_HEIMS, '') || ',' ||
                   IFNULL(S.YEAR_LEFT_SCHOOL, '') || ',' ||
                   IFNULL(TO_CHAR(S.DATE_OF_BIRTH, 'YYYYMMDD'), '') || ',' ||
                   IFNULL(S.QUALIFICATION, '') || ',' ||
                   IFNULL(S.CURRENT_ROUND_MQ_PREFERENCE_SUMMARY, 0) || ',' ||
                   IFNULL(S.CURRENT_ROUND_TOTAL_PREFERENCE_SUMMARY, 0) || ',' ||
                   IFNULL(S.CURRENT_ROUND_MQ_HIGHEST_PREFERENCE_NUMBER, 0) || ',' ||
                   IFNULL(S.MQ_OFFER_SUMMARY, 0) || ',' ||
                   IFNULL(S.TOTAL_OFFER_SUMMARY, 0) || ',' ||
                   IFNULL(S.GENDER, '') || ',' ||
                   IFNULL(S.CURRENT_APPLICATION, '') || ',' ||
                   IFNULL(S.SCHOOL_CODE, '') || ',' ||
                   IFNULL(S.YEAR_OF_APPLICATION, '') || ',' ||
                   IFNULL(S.APPLICANT_REFERENCE_NUMBER, 0) || ',' ||
                   IFNULL(S.GIVEN_NAME, '') || ',' ||
                   IFNULL(S.SURNAME, '') || ',' ||
                   IFNULL(S.HOME_ADDRESS, '') || ',' ||
                   IFNULL(S.HOME_SUBURB, '') || ',' ||
                   IFNULL(S.HOME_STATE, '') || ',' ||
                   IFNULL(S.HOME_POST_CODE, '') || ',' ||
                   IFNULL(S.PERMANENT_STATE, '') || ',' ||
                   IFNULL(S.PERMANENT_POST_CODE, '') || ',' ||
                   IFNULL(S.PERMANENT_ADDRESS, '') || ',' ||
                   IFNULL(S.PERMANENT_SUBURB, '') || ',' ||
                   IFNULL(S.EMAIL, '') || ',' ||
                   IFNULL(S.PHONE, '') || ',' ||
                   IFNULL(S.STATUS_TYPE, '') || ',' ||
                   IFNULL(S.CITIZENSHIP_CODE, '') || ',' ||
                   IFNULL(S.CITIZENSHIP, '') || ',' ||
                   IFNULL(S.NATIONAL_LANGUAGE_CODE, '') || ',' ||
                   IFNULL(S.NATIONAL_LANGUAGE, '') || ',' ||
                   IFNULL(S.BASE_RANK_SET_VALUE, '') || ',' ||
                   IFNULL(S.HIGHEST_QUALIFICATION_CODE, '') || ',' ||
                   IFNULL(S.CHESSN, '') || ',' ||
                   IFNULL(TO_CHAR(S.ENTITLEMENT_CONFIRMATION_DATE, 'YYYYMMDD'), '') || ',' ||
                   IFNULL(TO_CHAR(S.RECEIVE_DATE, 'YYYYMMDD'), '') || ',' ||
                   IFNULL(TO_CHAR(S.DATA_ENTRY_DATE, 'YYYYMMDD'), '') || ',' ||
                   IFNULL(S.COUNTRY_ORIGIN_CODE, '') || ',' ||
                   IFNULL(S.COUNTRY_ORIGIN, '') || ',' ||
                   IFNULL(S.SCHOOL_NAME, '') || ',' ||
                   IFNULL(S.SCHOOL_VET_FLAG, '') || ',' ||
                   IFNULL(S.SCHOOL_OFFSHORE, '') || ',' ||
                   IFNULL(S.SCHOOL_COUNTRY, '') || ',' ||
                   IFNULL(S.SCHOOL_POSTAL_CODE, '') || ',' ||
                   IFNULL(S.SCHOOL_STATE, '') || ',' ||
                   IFNULL(S.SCHOOL_ADDRESS, '') || ',' ||
                   IFNULL(S.SCHOOL_SUBURB, '') || ',' ||
                   IFNULL(S.TITLE, '') || ',' ||
                   IFNULL(S.APPLICATION_STATUS, '') || ',' ||
                   IFNULL(S.SCHOOL_MESH_BLOCK, '') || ',' ||
                   IFNULL(S.SCHOOL_LATITUDE, 0) || ',' ||
                   IFNULL(S.SCHOOL_LONGITUDE, 0) || ',' ||
                   IFNULL(S.ALTERNATE_PHONE, '') || ',' ||
                   IFNULL(S.IS_DELETED, '')
           )                       HASH_MD5,
       S.ATAR,
       S.SCHOOL_LEAVER,
       S.CURRENT_SCHOOL_LEAVER,
       S.RECENT_SCHOOL_LEAVER,
       S.RECENT_SCHOOL_LEAVER_HEIMS,
       S.YEAR_LEFT_SCHOOL,
       S.DATE_OF_BIRTH,
       S.QUALIFICATION,
       S.CURRENT_ROUND_MQ_PREFERENCE_SUMMARY,
       S.CURRENT_ROUND_TOTAL_PREFERENCE_SUMMARY,
       S.CURRENT_ROUND_MQ_HIGHEST_PREFERENCE_NUMBER,
       S.MQ_OFFER_SUMMARY,
       S.TOTAL_OFFER_SUMMARY,
       S.GENDER,
       S.CURRENT_APPLICATION,
       S.SCHOOL_CODE,
       S.YEAR_OF_APPLICATION,
       S.APPLICANT_REFERENCE_NUMBER,
       S.GIVEN_NAME,
       S.SURNAME,
       S.HOME_ADDRESS,
       S.HOME_SUBURB,
       S.HOME_STATE,
       S.HOME_POST_CODE,
       S.PERMANENT_STATE,
       S.PERMANENT_POST_CODE,
       S.PERMANENT_ADDRESS,
       S.PERMANENT_SUBURB,
       S.EMAIL,
       S.PHONE,
       S.STATUS_TYPE,
       S.CITIZENSHIP_CODE,
       S.CITIZENSHIP,
       S.NATIONAL_LANGUAGE_CODE,
       S.NATIONAL_LANGUAGE,
       S.BASE_RANK_SET_VALUE,
       S.HIGHEST_QUALIFICATION_CODE,
       S.CHESSN,
       S.ENTITLEMENT_CONFIRMATION_DATE,
       S.RECEIVE_DATE,
       S.DATA_ENTRY_DATE,
       S.COUNTRY_ORIGIN_CODE,
       S.COUNTRY_ORIGIN,
       S.SCHOOL_NAME,
       S.SCHOOL_VET_FLAG,
       S.SCHOOL_OFFSHORE,
       S.SCHOOL_COUNTRY,
       S.SCHOOL_POSTAL_CODE,
       S.SCHOOL_STATE,
       S.SCHOOL_ADDRESS,
       S.SCHOOL_SUBURB,
       S.TITLE,
       S.APPLICATION_STATUS,
       S.SCHOOL_MESH_BLOCK,
       S.SCHOOL_LATITUDE,
       S.SCHOOL_LONGITUDE,
       S.ALTERNATE_PHONE,
       S.IS_DELETED
FROM (
         SELECT MD5(IFNULL(APPLIC.REFNUM, 0) || ',' ||
                    APPLIC.YEAR || ',' ||
                    APPLIC.SOURCE)                                    HUB_UAC_APPLICANT_KEY,
                APPLIC.SOURCE                                         SOURCE,
                CURRENT_TIMESTAMP::TIMESTAMP_NTZ                      LOAD_DTS,
                'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ             ETL_JOB_ID,
                ROUND(RELEVANT_QUAL.ATAR::FLOAT, 2)                   ATAR,
                CASE
                    --                     WHEN APPLIC.SOURCE LIKE '%ALLPGP%' THEN 'School-leaver'
                    WHEN APPLIC.REFNUM NOT LIKE '9%' THEN 'School-leaver'
                    ELSE 'Non-school-applicant' END                   SCHOOL_LEAVER,

                IFNULL(SCHOOL_LEAVER.CURRENT_SCHOOL_LEAVER, 'N')      CURRENT_SCHOOL_LEAVER,
                IFNULL(SCHOOL_LEAVER.RECENT_SCHOOL_LEAVER, 'N')       RECENT_SCHOOL_LEAVER,
                IFNULL(SCHOOL_LEAVER.RECENT_SCHOOL_LEAVER_HEIMS, 'N') RECENT_SCHOOL_LEAVER_HEIMS,
                RELEVANT_QUAL.YEAR                                    YEAR_LEFT_SCHOOL,
                APPLIC.DOB::TIMESTAMP_NTZ                             DATE_OF_BIRTH,
                RELEVANT_QUAL.CSTITLE                                 QUALIFICATION,
                PREFERENCE.CURRENT_ROUND_MQ_PREFERENCE_SUMMARY,
                PREFERENCE.CURRENT_ROUND_TOTAL_PREFERENCE_SUMMARY,
                PREFERENCE.CURRENT_ROUND_MQ_HIGHEST_PREFERENCE_NUMBER,
                OFFER_SUM.MQ_OFFER_SUMMARY,
                OFFER_SUM.TOTAL_OFFER_SUMMARY,
                APPLIC.SEX                                            GENDER,
                IFF(APPLIC.FDSTATUS = 'C', 'Y', 'N')                  CURRENT_APPLICATION,
                SCHOOL.CODE                                           SCHOOL_CODE,
                APPLIC.YEAR                                           YEAR_OF_APPLICATION,
                APPLIC.REFNUM::NUMBER                                 APPLICANT_REFERENCE_NUMBER,
                APPLIC.GNAME1                                         GIVEN_NAME,
                APPLIC.SURNAME                                        SURNAME,
                APPLIC.ADDRESS1                                       HOME_ADDRESS,
                APPLIC.ADDRESS2                                       HOME_SUBURB,
                APPLIC.ADDRESS3                                       HOME_STATE,
                APPLIC.POSTCODE                                       HOME_POST_CODE,
                APPLIC.PADDR3                                         PERMANENT_STATE,
                APPLIC.PPCODE                                         PERMANENT_POST_CODE,
                APPLIC.PADDR1                                         PERMANENT_ADDRESS,
                APPLIC.PADDR2                                         PERMANENT_SUBURB,
                APPLIC.EMAIL                                          EMAIL,
                APPLIC.PHONE                                          PHONE,
                APPLIC.STATTYPE                                       STATUS_TYPE,
                LPAD(APPLIC.CITIZ, 4, '0')                            CITIZENSHIP_CODE,
                CITIZENSHIP_CODE.CODE_DESCR                           CITIZENSHIP,
                APPLIC.NATLANG                                        NATIONAL_LANGUAGE_CODE,
                LANGUAGE_CODE.CODE_DESCR                              NATIONAL_LANGUAGE,
                APPLIC.BRSVALUE                                       BASE_RANK_SET_VALUE,
                APPLIC.HIGHQUAL                                       HIGHEST_QUALIFICATION_CODE,
                APPLIC.CHESSN,
                APPLIC.VERDATE::TIMESTAMP_NTZ                         ENTITLEMENT_CONFIRMATION_DATE,
                APPLIC.RECDATE::TIMESTAMP_NTZ                         RECEIVE_DATE,
                APPLIC.ENTDATE::TIMESTAMP_NTZ                         DATA_ENTRY_DATE,
                APPLIC.CITORIG                                        COUNTRY_ORIGIN_CODE,
                COUNTRY_CODE.CODE_DESCR                               COUNTRY_ORIGIN,
                INITCAP(SCHOOL.NAME)                                  SCHOOL_NAME,
                SCHOOL.VETFLAG                                        SCHOOL_VET_FLAG,
                SCHOOL.OFFSHORE                                       SCHOOL_OFFSHORE,
                SCHOOL_COUNTRY_CODE.CODE_DESCR                        SCHOOL_COUNTRY,
                SCHOOL.SCHPCODE                                       SCHOOL_POSTAL_CODE,
                SCHOOL.STATE                                          SCHOOL_STATE,
                SCHOOL.SCHADDR1                                       SCHOOL_ADDRESS,
                SCHOOL.SCHADDR2                                       SCHOOL_SUBURB,
                APPLIC.TITLE                                          TITLE,
                CASE
                    WHEN APPLIC.FDSTATUS = 'C' THEN 'Current application'
                    WHEN APPLIC.FDSTATUS = 'A' THEN 'Application withdrawn by applicant'
                    WHEN APPLIC.FDSTATUS = 'D' THEN 'Application cancelled by UAC as a duplicate'
                    WHEN APPLIC.FDSTATUS = 'U' THEN 'Application cancelled by UAC'
                    WHEN APPLIC.FDSTATUS = 'X' THEN 'Application cancelled by UAC as applicant deceased'
                    WHEN APPLIC.FDSTATUS = 'E' THEN 'Expired application'
                    END                                               APPLICATION_STATUS,
                UAC_SCHOOL_GEO.MESH_BLOCK                             SCHOOL_MESH_BLOCK,
                UAC_SCHOOL_GEO.LATITUDE                               SCHOOL_LATITUDE,
                UAC_SCHOOL_GEO.LONGITUDE                              SCHOOL_LONGITUDE,
                APPLIC.PHONEALT                                       ALTERNATE_PHONE,
                'N'                                                   IS_DELETED
         FROM ODS.UAC.VW_ALL_APPLIC APPLIC
                  LEFT OUTER JOIN (SELECT A.SOURCE,
                                          A.YEAR,
                                          A.REFNUM,
                                          A.QUALNUM,
                                          A.LVEL,
                                          A.ORIGIN,
                                          A.WHENCE,
                                          A.YEAR_,
                                          A.ATAR,
                                          A.CSTITLE
                                   FROM (SELECT SOURCE,
                                                YEAR,
                                                REFNUM,
                                                QUALNUM,
                                                LVEL,
                                                ORIGIN,
                                                WHENCE,
                                                Year_,
                                                SCHEDULE,
                                                RANK                                                                          ATAR,
                                                CSTITLE,
                                                RANK()
                                                        OVER (PARTITION BY SOURCE, YEAR, REFNUM ORDER BY YEAR_ DESC, QUALNUM) ORD
                                         FROM ODS.UAC.VW_ALL_QUAL
                                         where TRIM(SCHEDULE) <> 'na'
                                           and LVEL like 'S%'
                                        ) A
                                   Where A.ORD = 1) RELEVANT_QUAL
                                  ON RELEVANT_QUAL.REFNUM = APPLIC.REFNUM
                                      AND RELEVANT_QUAL.SOURCE = APPLIC.SOURCE
                                      AND RELEVANT_QUAL.YEAR = APPLIC.YEAR
                  LEFT OUTER JOIN ODS.UAC.VW_ALL_SCHOOL SCHOOL
                                  ON SCHOOL.CODE = RELEVANT_QUAL.WHENCE
                                      AND SCHOOL.STATE = RELEVANT_QUAL.ORIGIN
                                      AND SCHOOL.SOURCE = RELEVANT_QUAL.SOURCE
                                      AND SCHOOL.YEAR = RELEVANT_QUAL.YEAR
                  LEFT OUTER JOIN (
             SELECT PREF.SOURCE,
                    PREF.YEAR,
                    PREF.REFNUM,
                    SUM(CASE WHEN COURSE.INSTCODE = 'MQ' THEN 1 ELSE 0 END)               CURRENT_ROUND_MQ_PREFERENCE_SUMMARY,
                    SUM(1)                                                                CURRENT_ROUND_TOTAL_PREFERENCE_SUMMARY,
                    MIN(CASE WHEN COURSE.INSTCODE = 'MQ' THEN PREF.PREFNUM ELSE NULL END) CURRENT_ROUND_MQ_HIGHEST_PREFERENCE_NUMBER
             FROM ODS.UAC.VW_ALL_PREF PREF
                      LEFT OUTER JOIN ODS.UAC.VW_ALL_COURSE COURSE
                                      ON COURSE.COURSE = PREF.COURSE
                                          AND COURSE.SOURCE = PREF.SOURCE
                                          AND COURSE.YEAR = PREF.YEAR
             WHERE PREF.SETNUM = 0
             GROUP BY PREF.SOURCE, PREF.YEAR, PREF.REFNUM) PREFERENCE
                                  ON PREFERENCE.REFNUM = APPLIC.REFNUM
                                      AND PREFERENCE.SOURCE = APPLIC.SOURCE
                                      AND PREFERENCE.YEAR = APPLIC.YEAR
                  LEFT OUTER JOIN (
             SELECT OFFER.SOURCE,
                    OFFER.YEAR,
                    OFFER.REFNUM,
                    SUM(CASE WHEN COURSE.INSTCODE = 'MQ' THEN 1 ELSE 0 END) MQ_OFFER_SUMMARY,
                    SUM(1)                                                  TOTAL_OFFER_SUMMARY
             FROM ODS.UAC.VW_ALL_OFFER OFFER
                      JOIN ODS.UAC.VW_ALL_COURSE COURSE
                           ON COURSE.COURSE = OFFER.COURSE
                               AND COURSE.SOURCE = OFFER.SOURCE
                               AND COURSE.YEAR = OFFER.YEAR
             GROUP BY OFFER.SOURCE, OFFER.YEAR, OFFER.REFNUM
         ) OFFER_SUM
                                  ON OFFER_SUM.REFNUM = APPLIC.REFNUM
                                      AND OFFER_SUM.SOURCE = APPLIC.SOURCE
                                      AND OFFER_SUM.YEAR = APPLIC.YEAR
                  LEFT OUTER JOIN (SELECT F.SOURCE,
                                          F.YEAR,
                                          F.REFNUM,
                                          MAX(CASE WHEN F.FACTCODE = 'CSL' THEN 'Y' ELSE 'N' END)  CURRENT_SCHOOL_LEAVER,
                                          MAX(CASE WHEN F.FACTCODE = 'RSL' THEN 'Y' ELSE 'N' END)  RECENT_SCHOOL_LEAVER,
                                          MAX(CASE WHEN F.FACTCODE = 'RSLH' THEN 'Y' ELSE 'N' END) RECENT_SCHOOL_LEAVER_HEIMS
                                   FROM ODS.UAC.VW_ALL_FACT F
                                   GROUP BY F.SOURCE, F.YEAR, F.REFNUM) SCHOOL_LEAVER
                                  ON SCHOOL_LEAVER.REFNUM = APPLIC.REFNUM
                                      AND SCHOOL_LEAVER.SOURCE = APPLIC.SOURCE
                                      AND SCHOOL_LEAVER.YEAR = APPLIC.YEAR
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE CITIZENSHIP_CODE
                                  ON CITIZENSHIP_CODE.CODE_ID = APPLIC.CITIZ AND
                                     CITIZENSHIP_CODE.CODE_TYPE = 'CITIZENSHIP'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE LANGUAGE_CODE
                                  ON LANGUAGE_CODE.CODE_ID = LPAD(APPLIC.NATLANG, 4, '0') AND
                                     LANGUAGE_CODE.CODE_TYPE = 'HOME_LANGUAGE'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE COUNTRY_CODE
                                  ON 'X' || COUNTRY_CODE.CODE_ID = APPLIC.CITORIG AND COUNTRY_CODE.CODE_TYPE = 'COUNTRY'
                  LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SCHOOL_COUNTRY_CODE
                                  ON 'X' || SCHOOL_COUNTRY_CODE.CODE_ID = SCHOOL.CNTRY AND
                                     SCHOOL_COUNTRY_CODE.CODE_TYPE = 'COUNTRY'
                  LEFT OUTER JOIN ODS.EXT_REF.UAC_SCHOOL_GEO UAC_SCHOOL_GEO
                                  ON UPPER(UAC_SCHOOL_GEO.SCHOOL_NAME) = UPPER(SCHOOL.NAME)
                                      AND UAC_SCHOOL_GEO.SCHOOL_POST_CODE = SCHOOL.SCHPCODE
                                      AND UAC_SCHOOL_GEO.SCHOOL_STATE = SCHOOL.STATE
     ) S
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_UAC_APPLICANT_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_UAC_APPLICANT_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_UAC_APPLICANT_SUM) SAT
        WHERE SAT.EFFECTIVE_END_DTS IS NULL
          AND SAT.HUB_UAC_APPLICANT_KEY = S.HUB_UAC_APPLICANT_KEY
          AND SAT.HASH_MD5 = MD5(
                    IFNULL(S.ATAR, 0) || ',' ||
                    IFNULL(S.SCHOOL_LEAVER, '') || ',' ||
                    IFNULL(S.CURRENT_SCHOOL_LEAVER, '') || ',' ||
                    IFNULL(S.RECENT_SCHOOL_LEAVER, '') || ',' ||
                    IFNULL(S.RECENT_SCHOOL_LEAVER_HEIMS, '') || ',' ||
                    IFNULL(S.YEAR_LEFT_SCHOOL, '') || ',' ||
                    IFNULL(TO_CHAR(S.DATE_OF_BIRTH, 'YYYYMMDD'), '') || ',' ||
                    IFNULL(S.QUALIFICATION, '') || ',' ||
                    IFNULL(S.CURRENT_ROUND_MQ_PREFERENCE_SUMMARY, 0) || ',' ||
                    IFNULL(S.CURRENT_ROUND_TOTAL_PREFERENCE_SUMMARY, 0) || ',' ||
                    IFNULL(S.CURRENT_ROUND_MQ_HIGHEST_PREFERENCE_NUMBER, 0) || ',' ||
                    IFNULL(S.MQ_OFFER_SUMMARY, 0) || ',' ||
                    IFNULL(S.TOTAL_OFFER_SUMMARY, 0) || ',' ||
                    IFNULL(S.GENDER, '') || ',' ||
                    IFNULL(S.CURRENT_APPLICATION, '') || ',' ||
                    IFNULL(S.SCHOOL_CODE, '') || ',' ||
                    IFNULL(S.YEAR_OF_APPLICATION, '') || ',' ||
                    IFNULL(S.APPLICANT_REFERENCE_NUMBER, 0) || ',' ||
                    IFNULL(S.GIVEN_NAME, '') || ',' ||
                    IFNULL(S.SURNAME, '') || ',' ||
                    IFNULL(S.HOME_ADDRESS, '') || ',' ||
                    IFNULL(S.HOME_SUBURB, '') || ',' ||
                    IFNULL(S.HOME_STATE, '') || ',' ||
                    IFNULL(S.HOME_POST_CODE, '') || ',' ||
                    IFNULL(S.PERMANENT_STATE, '') || ',' ||
                    IFNULL(S.PERMANENT_POST_CODE, '') || ',' ||
                    IFNULL(S.PERMANENT_ADDRESS, '') || ',' ||
                    IFNULL(S.PERMANENT_SUBURB, '') || ',' ||
                    IFNULL(S.EMAIL, '') || ',' ||
                    IFNULL(S.PHONE, '') || ',' ||
                    IFNULL(S.STATUS_TYPE, '') || ',' ||
                    IFNULL(S.CITIZENSHIP_CODE, '') || ',' ||
                    IFNULL(S.CITIZENSHIP, '') || ',' ||
                    IFNULL(S.NATIONAL_LANGUAGE_CODE, '') || ',' ||
                    IFNULL(S.NATIONAL_LANGUAGE, '') || ',' ||
                    IFNULL(S.BASE_RANK_SET_VALUE, '') || ',' ||
                    IFNULL(S.HIGHEST_QUALIFICATION_CODE, '') || ',' ||
                    IFNULL(S.CHESSN, '') || ',' ||
                    IFNULL(TO_CHAR(S.ENTITLEMENT_CONFIRMATION_DATE, 'YYYYMMDD'), '') || ',' ||
                    IFNULL(TO_CHAR(S.RECEIVE_DATE, 'YYYYMMDD'), '') || ',' ||
                    IFNULL(TO_CHAR(S.DATA_ENTRY_DATE, 'YYYYMMDD'), '') || ',' ||
                    IFNULL(S.COUNTRY_ORIGIN_CODE, '') || ',' ||
                    IFNULL(S.COUNTRY_ORIGIN, '') || ',' ||
                    IFNULL(S.SCHOOL_NAME, '') || ',' ||
                    IFNULL(S.SCHOOL_VET_FLAG, '') || ',' ||
                    IFNULL(S.SCHOOL_OFFSHORE, '') || ',' ||
                    IFNULL(S.SCHOOL_COUNTRY, '') || ',' ||
                    IFNULL(S.SCHOOL_POSTAL_CODE, '') || ',' ||
                    IFNULL(S.SCHOOL_STATE, '') || ',' ||
                    IFNULL(S.SCHOOL_ADDRESS, '') || ',' ||
                    IFNULL(S.SCHOOL_SUBURB, '') || ',' ||
                    IFNULL(S.TITLE, '') || ',' ||
                    IFNULL(S.APPLICATION_STATUS, '') || ',' ||
                    IFNULL(S.SCHOOL_MESH_BLOCK, '') || ',' ||
                    IFNULL(S.SCHOOL_LATITUDE, 0) || ',' ||
                    IFNULL(S.SCHOOL_LONGITUDE, 0) || ',' ||
                    IFNULL(S.ALTERNATE_PHONE, '') || ',' ||
                    IFNULL(S.IS_DELETED, '')
            )
    );

