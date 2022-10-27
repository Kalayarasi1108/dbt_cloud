INSERT INTO DATA_VAULT.CORE.HUB_ORG_UNIT(HUB_ORG_UNIT_KEY, ORG_UNIT_CD, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT ORG_UNIT.HUB_ORG_UNIT_KEY,
       ORG_UNIT.ORG_UNIT_CD,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM (
         SELECT MD5(IFNULL(ORG_UNIT_CD, '')
                    )                                                   HUB_ORG_UNIT_KEY,
                ORG_UNIT_CD,
                ROW_NUMBER() OVER (PARTITION BY ORG_UNIT_CD ORDER BY 1) RN
         from ODS.AMIS.S1ORG_UNIT ORG_UNIT
     ) ORG_UNIT
WHERE ORG_UNIT.RN = 1
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.HUB_ORG_UNIT H
        WHERE H.HUB_ORG_UNIT_KEY =
              MD5(IFNULL(ORG_UNIT.ORG_UNIT_CD, '')
                  )
    )
;