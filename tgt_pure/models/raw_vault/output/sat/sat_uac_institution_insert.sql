-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_UAC_INSTITUTION (SAT_UAC_INSTITUTION_SK, HUB_UAC_INSTITUTION_KEY, SOURCE,
                                                     LOAD_DTS, ETL_JOB_ID, HASH_MD5, YEAR, CODE, MNEMONIC, SHORTNAM,
                                                     LONGNAM, TYPE, ORIGIN, CURROFF, ESPART, EXMODEPG, EXMODEOS,
                                                     IS_DELETED)

WITH SAT_UAC_INSTITUTE AS (
    SELECT HUB_UAC_INSTITUTION_KEY,
           HASH_MD5,
           SOURCE,
           LOAD_DTS,
           LEAD(LOAD_DTS)
                OVER (PARTITION BY HUB_UAC_INSTITUTION_KEY,HASH_MD5 ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
    FROM DATA_VAULT.CORE.SAT_UAC_INSTITUTION
)
   , UNIQUE_INST_DATA AS (
    SELECT YEAR,
           CODE,
           SOURCE,
           MNEMONIC,
           SHORTNAM,
           LONGNAM,
           TYPE,
           ORIGIN,
           CURROFF,
           ESPART,
           EXMODEOS,
           EXMODEPG
    FROM ODS.UAC.VW_ALL_INST
)
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_UAC_INSTITUTION_SK,
       MD5(
                   IFNULL(CODE, '') || ',' ||
                   IFNULL(YEAR, '') || ',' ||
                   IFNULL(SOURCE, '')
           )                                     HUB_UAC_INSTITUTION_KEY,
       SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(
                   IFNULL(SOURCE, '') || ',' ||
                   IFNULL(CODE, '') || ',' ||
                   IFNULL(YEAR, '') || ',' ||
                   IFNULL(MNEMONIC, '') || ',' ||
                   IFNULL(SHORTNAM, '') || ',' ||
                   IFNULL(LONGNAM, '') || ',' ||
                   IFNULL(TYPE, '') || ',' ||
                   IFNULL(ORIGIN, '') || ',' ||
                   IFNULL(CURROFF, '') || ',' ||
                   IFNULL(ESPART, '') || ',' ||
                   IFNULL(EXMODEOS, '') || ',' ||
                   IFNULL(EXMODEPG, '') || ',' ||
                   IFNULL('N', '')
           )                                     HASH_MD5,
       YEAR,
       CODE,
       MNEMONIC,
       SHORTNAM,
       LONGNAM,
       TYPE,
       ORIGIN,
       CURROFF,
       ESPART,
       EXMODEOS,
       EXMODEPG,
       'N'                                       IS_DELETED
FROM UNIQUE_INST_DATA INST
WHERE NOT EXISTS
    (
        SELECT NULL
        FROM SAT_UAC_INSTITUTE SAT
        WHERE SAT.EFFECTIVE_END_DTS IS NULL
          AND SAT.SOURCE = INST.SOURCE
          AND SAT.HUB_UAC_INSTITUTION_KEY = MD5(
                    IFNULL(INST.CODE, '') || ',' ||
                    IFNULL(INST.YEAR, '') || ',' ||
                    IFNULL(INST.SOURCE, '')
            )
          AND SAT.HASH_MD5 = MD5(
                    IFNULL(INST.SOURCE, '') || ',' ||
                    IFNULL(INST.CODE, '') || ',' ||
                    IFNULL(INST.YEAR, '') || ',' ||
                    IFNULL(INST.MNEMONIC, '') || ',' ||
                    IFNULL(INST.SHORTNAM, '') || ',' ||
                    IFNULL(INST.LONGNAM, '') || ',' ||
                    IFNULL(INST.TYPE, '') || ',' ||
                    IFNULL(INST.ORIGIN, '') || ',' ||
                    IFNULL(INST.CURROFF, '') || ',' ||
                    IFNULL(INST.ESPART, '') || ',' ||
                    IFNULL(INST.EXMODEOS, '') || ',' ||
                    IFNULL(INST.EXMODEPG, '') || ',' ||
                    IFNULL('N', '')
            )
    );