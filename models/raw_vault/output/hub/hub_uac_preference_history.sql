INSERT INTO DATA_VAULT.CORE.HUB_UAC_PREFERENCE_HISTORY (HUB_UAC_PREFERENCE_HISTORY_KEY,
                                                        YEAR,
                                                        SOURCE,
                                                        LOAD_DTS,
                                                        ETL_JOB_ID,
                                                        STATDATE,
                                                        STATGRP,
                                                        PREFNUM,
                                                        COURSE)
SELECT MD5(
                   IFNULL(TO_CHAR(A.STATDATE, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(A.STATGRP, '') || ',' ||
                   IFNULL(A.PREFNUM, '') || ',' ||
                   IFNULL(A.COURSE, '') || ',' ||
                   A.YEAR || ',' ||
                   A.SOURCE)                     HUB_UAC_PREFERENCE_HISTORY_KEY,
       A.YEAR                                    YEAR,
       A.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       A.STATDATE,
       A.STATGRP,
       A.PREFNUM,
       A.COURSE                                  COURSE
FROM (
         SELECT GENSTAT.YEAR,
                GENSTAT.SOURCE,
                GENSTAT.STATDATE::DATE STATDATE,
                GENSTAT.STATGRP,
                GENSTAT.COURSE,
                GENSTAT.PREFNUM
         FROM ODS.UAC.VW_ALL_GENSTAT1 GENSTAT
     ) A
WHERE NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.HUB_UAC_PREFERENCE_HISTORY S
        WHERE S.HUB_UAC_PREFERENCE_HISTORY_KEY = MD5(
                    IFNULL(TO_CHAR(A.STATDATE, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(A.STATGRP, '') || ',' ||
                    IFNULL(A.PREFNUM, '') || ',' ||
                    IFNULL(A.COURSE, '') || ',' ||
                    A.YEAR || ',' ||
                    A.SOURCE)
    );