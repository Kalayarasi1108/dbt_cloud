INSERT INTO DATA_VAULT.CORE.HUB_THESIS_SUBMISSION (HUB_THESIS_SUBMISSION_KEY,
                                                   STU_ID,
                                                   SSP_NO,
                                                   SSP_WRK_SUBM_NO,
                                                   SOURCE,
                                                   LOAD_DTS,
                                                   ETL_JOB_ID)
SELECT MD5(IFNULL(STU_SPK.STU_ID, '') || ',' ||
           IFNULL(STU_SPK_WS.SSP_NO, 0) || ',' ||
           IFNULL(STU_SPK_WS.SSP_WRK_SUBM_NO, 0)
           )                                     HUB_THESIS_SUBMISSION_KEY,
       STU_SPK.STU_ID,
       STU_SPK_WS.SSP_NO,
       STU_SPK_WS.SSP_WRK_SUBM_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SSP_WRKSUBM_DTL STU_SPK_WS
         INNER JOIN ODS.AMIS.S1SSP_STU_SPK STU_SPK
                    ON STU_SPK_WS.SSP_NO = STU_SPK.SSP_NO
WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.HUB_THESIS_SUBMISSION H
        WHERE H.HUB_THESIS_SUBMISSION_KEY = MD5(IFNULL(STU_SPK.STU_ID, '') || ',' ||
                                                IFNULL(STU_SPK_WS.SSP_NO, 0) || ',' ||
                                                IFNULL(STU_SPK_WS.SSP_WRK_SUBM_NO, 0)
            )
    )
;
