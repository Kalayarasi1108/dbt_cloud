INSERT INTO DATA_VAULT.CORE.LNK_UAC_OFFER_TO_PREFERENCE (LNK_UAC_OFFER_TO_PREFERENCE_KEY, HUB_UAC_OFFER_KEY,
                                                         HUB_UAC_PREFERENCE_KEY, ROUNDNUM, REFNUM, COURSE, PREFNUM,
                                                         SETNUM, YEAR, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(IFNULL(OFFER.ROUNDNUM, 0) || ',' ||
           IFNULL(OFFER.REFNUM, 0) || ',' ||
           IFNULL(OFFER.COURSE, 0) || ',' ||
           IFNULL(PREF.PREFNUM, 0) || ',' ||
           IFNULL(PREF.SETNUM, 0) || ',' ||
           OFFER.YEAR || ',' ||
           OFFER.SOURCE)                         LNK_UAC_OFFER_TO_PREFERENCE_KEY,
       MD5(IFNULL(OFFER.ROUNDNUM, 0) || ',' ||
           IFNULL(OFFER.REFNUM, 0) || ',' ||
           IFNULL(OFFER.COURSE, 0) || ',' ||
           OFFER.YEAR || ',' ||
           OFFER.SOURCE)                         HUB_UAC_OFFER_KEY,
       MD5(IFNULL(PREF.PREFNUM, 0) || ',' ||
           IFNULL(PREF.SETNUM, 0) || ',' ||
           IFNULL(PREF.REFNUM, 0) || ',' ||
           IFNULL(PREF.COURSE, 0) || ',' ||
           PREF.YEAR || ',' ||
           PREF.SOURCE)                          HUB_UAC_PREFERENCE_KEY,
       OFFER.ROUNDNUM,
       OFFER.REFNUM,
       OFFER.COURSE,
       PREF.PREFNUM,
       PREF.SETNUM,
       OFFER.YEAR,
       OFFER.SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.UAC.VW_ALL_OFFER OFFER
         JOIN ODS.UAC.VW_ALL_PREF PREF
              ON OFFER.SOURCE = PREF.SOURCE
                  AND OFFER.YEAR = PREF.YEAR
                  AND OFFER.REFNUM = PREF.REFNUM
                  AND OFFER.COURSE = PREF.COURSE
WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_UAC_OFFER_TO_PREFERENCE L
        WHERE L.HUB_UAC_OFFER_KEY = MD5(IFNULL(OFFER.ROUNDNUM, 0) || ',' ||
                                        IFNULL(OFFER.REFNUM, 0) || ',' ||
                                        IFNULL(OFFER.COURSE, 0) || ',' ||
                                        OFFER.YEAR || ',' ||
                                        OFFER.SOURCE)
          AND L.HUB_UAC_PREFERENCE_KEY = MD5(IFNULL(PREF.PREFNUM, 0) || ',' ||
                                             IFNULL(PREF.SETNUM, 0) || ',' ||
                                             IFNULL(PREF.REFNUM, 0) || ',' ||
                                             IFNULL(PREF.COURSE, 0) || ',' ||
                                             PREF.YEAR || ',' ||
                                             PREF.SOURCE)
    );
