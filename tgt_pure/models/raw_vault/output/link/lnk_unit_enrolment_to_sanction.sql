INSERT INTO DATA_VAULT.CORE.LNK_UNIT_ENROLMENT_TO_SANCTION (LNK_UNIT_ENROLMENT_TO_SANCTION_KEY, HUB_UNIT_ENROLMENT_KEY,
                                                            HUB_UNIT_ENROLMENT_SANCTION_KEY, STU_ID, SSP_NO,
                                                            UNIT_SPK_CD, UNIT_SPK_VER_NO, UNIT_AVAIL_YR,
                                                            UNIT_LOCATION_CD, UNIT_SPRD_CD, UNIT_AVAIL_KEY_NO,
                                                            UNIT_PARENT_SSP_NO, SANCTION_SEQ_NO, SOURCE, LOAD_DTS,
                                                            ETL_JOB_ID)
SELECT MD5(
                   IFNULL(STU_SPK.STU_ID, '') || ',' ||
                   IFNULL(STU_SPK.SSP_NO, 0) || ',' ||
                   IFNULL(SPK_DET.SPK_CD, '') || ',' ||
                   IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
                   IFNULL(STU_SPK.AVAIL_YR, 0) || ',' ||
                   IFNULL(STU_SPK.LOCATION_CD, '') || ',' ||
                   IFNULL(STU_SPK.SPRD_CD, '') || ',' ||
                   IFNULL(STU_SPK.AVAIL_KEY_NO, 0) || ',' ||
                   IFNULL(STU_SPK.SSP_NO, 0) || ',' ||
                   IFNULL(STU_SPK.PARENT_SSP_NO, 0) || ',' ||
                   IFNULL(SCT_DTL.SEQ_NO, 0)
           )                                     LNK_UNIT_ENROLMENT_TO_SANCTION_KEY,

       MD5(
                   IFNULL(STU_SPK.STU_ID, '') || ',' ||
                   IFNULL(SPK_DET.SPK_CD, '') || ',' ||
                   IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
                   IFNULL(STU_SPK.AVAIL_YR, 0) || ',' ||
                   IFNULL(STU_SPK.LOCATION_CD, '') || ',' ||
                   IFNULL(STU_SPK.SPRD_CD, '') || ',' ||
                   IFNULL(STU_SPK.AVAIL_KEY_NO, 0) || ',' ||
                   IFNULL(STU_SPK.PARENT_SSP_NO, 0) || ',' ||
                   IFNULL(SCT_DTL.SEQ_NO, 0)
           )                                     HUB_UNIT_ENROLMENT_KEY,
       MD5(
                   IFNULL(SCT_DTL.SSP_NO, 0) || ',' ||
                   IFNULL(SCT_DTL.SEQ_NO, 0) || ',' ||
                   IFNULL(SCT_DTL.STU_ID, '')
           )                                     HUB_UNIT_ENROLMENT_SANCTION_KEY,
       STU_SPK.STU_ID,
       STU_SPK.SSP_NO,
       SPK_DET.SPK_CD                            UNIT_SPK_CD,
       SPK_DET.SPK_VER_NO                        UNIT_SPK_VER_NO,
       STU_SPK.AVAIL_YR                          UNIT_AVAIL_YR,
       STU_SPK.LOCATION_CD                       UNIT_LOCATION_CD,
       STU_SPK.SPRD_CD                           UNIT_SPRD_CD,
       STU_SPK.AVAIL_KEY_NO                      UNIT_AVAIL_KEY_NO,
       STU_SPK.PARENT_SSP_NO                     UNIT_PARENT_SSP_NO,
       SCT_DTL.SEQ_NO                            SANCTION_SEQ_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SSP_SCT_DTL SCT_DTL
         JOIN ODS.AMIS.S1SSP_STU_SPK STU_SPK ON STU_SPK.SSP_NO = SCT_DTL.SSP_NO
         JOIN ODS.AMIS.S1SPK_DET SPK_DET ON STU_SPK.SPK_NO = SPK_DET.SPK_NO and STU_SPK.SPK_VER_NO = SPK_DET.SPK_VER_NO
    AND SPK_DET.SPK_CAT_CD = 'UN'

WHERE STU_SPK.SSP_NO <> STU_SPK.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_UNIT_ENROLMENT_TO_SANCTION L
        WHERE L.LNK_UNIT_ENROLMENT_TO_SANCTION_KEY = MD5(
                    IFNULL(STU_SPK.STU_ID, '') || ',' ||
                    IFNULL(STU_SPK.SSP_NO, 0) || ',' ||
                    IFNULL(SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
                    IFNULL(STU_SPK.AVAIL_YR, 0) || ',' ||
                    IFNULL(STU_SPK.LOCATION_CD, '') || ',' ||
                    IFNULL(STU_SPK.SPRD_CD, '') || ',' ||
                    IFNULL(STU_SPK.AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(STU_SPK.SSP_NO, 0) || ',' ||
                    IFNULL(STU_SPK.PARENT_SSP_NO, 0) || ',' ||
                    IFNULL(SCT_DTL.SEQ_NO, 0)
            )
    );

