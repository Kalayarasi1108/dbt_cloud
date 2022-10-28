INSERT INTO DATA_VAULT.CORE.LNK_UAC_OFFER_TO_AMIS_COURSE_APPLICATION (LNK_UAC_OFFER_TO_AMIS_COURSE_APPLICATION_KEY,
                                                                                     HUB_UAC_OFFER_KEY,
                                                                                     HUB_COURSE_APPLICATION_KEY,
                                                                                     ROUNDNUM, REFNUM, COURSE, YEAR,
                                                                                     STU_ID, SPK_NO,
                                                                                     SPK_VER_NO, APPN_NO, SOURCE,
                                                                                     LOAD_DTS, ETL_JOB_ID)


SELECT MD5(IFNULL(ROUNDNUM, 0) || ',' ||
           IFNULL(REFNUM, 0) || ',' ||
           IFNULL(COURSE, 0) || ',' ||
           YEAR || ',' ||
           IFNULL(STU_ID, '') || ',' ||
           IFNULL(SPK_NO, 0) || ',' ||
           IFNULL(SPK_VER_NO, 0) || ',' ||
           IFNULL(CONCAT(Application_id, '_', APPLICATION_LINE_ID), '') || ',' ||
           SOURCE)                                         LNK_UAC_OFFER_TO_AMIS_COURSE_APPLICATION_KEY,
       MD5(IFNULL(ROUNDNUM, 0) || ',' ||
           IFNULL(REFNUM, 0) || ',' ||
           IFNULL(COURSE, 0) || ',' ||
           YEAR || ',' ||
           SOURCE)                                         HUB_UAC_OFFER_KEY,
       MD5(IFNULL(STU_ID, '') || ',' ||
           IFNULL(SPK_NO, 0) || ',' ||
           IFNULL(SPK_VER_NO, 0) || ',' ||
           IFNULL(CONCAT(Application_id, '_', APPLICATION_LINE_ID), ''))
                                                           HUB_COURSE_APPLICATION_KEY,
       ROUNDNUM,
       REFNUM,
       COURSE,
       YEAR,
       STU_ID,
       SPK_NO,
       SPK_VER_NO,
       CONCAT(Application_id, '_', APPLICATION_LINE_ID) AS APPN_NO,
       SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz                    LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ           ETL_JOB_ID
FROM (SELECT UAC_OFFER.ROUNDNUM,
             UAC_OFFER.REFNUM,
             UAC_OFFER.COURSE,
             UAC_OFFER.YEAR,
             UAC_OFFER.SOURCE,
             STU_APPLICATION.STU_ID,
             STU_APPLICATION.SPK_NO,
             STU_APPLICATION.SPK_VER_NO,
             STU_APPLICATION.APPN_NO,
             S.APPLICATION_ID,
             S.APPLICATION_LINE_ID,
             ROW_NUMBER() OVER (PARTITION BY STU_APPLICATION.ADMSN_CNTR_CRS_CD, STU_APPLICATION.ADMSN_CNTR_APP_ID
                 ORDER BY
                     UN_CENSUS_DT DESC NULLS LAST,
                     CASE CS_SSP.SSP_STTS_CD
                         WHEN 'ADM' THEN 1
                         WHEN 'POTC' THEN 2
                         WHEN 'LOA' THEN 3
                         WHEN 'PASS' THEN 4
                         WHEN 'AWOL' THEN 5
                         WHEN 'OFF' THEN 6
                         WHEN 'APL' THEN 7
                         WHEN 'WD' THEN 8
                         WHEN 'WDE' THEN 9
                         WHEN 'REPR' THEN 10
                         ELSE 99
                         END ASC,
                     CS_SSP.EFFCT_START_DT DESC, CS_SSP.SSP_ATT_NO DESC, CS_SSP.SSP_NO DESC,
                     STU_APPLICATION.APPN_NO DESC,
                     STU_APPLICATION.LAST_MOD_DATEI DESC,
                     STU_APPLICATION.LAST_MOD_TIMEI) RN,
             STU_DET.STU_CONSOL_FG
      FROM ODS.AMIS.S1APP_STUDY as S
               INNER JOIN
           ODS.AMIS.S1STU_APPLICATION STU_APPLICATION
           ON S.APPLICATION_ID = STU_APPLICATION.APPLICATION_ID
               and S.APPLICATION_LINE_ID = STU_APPLICATION.APPLICATION_LINE_ID
               AND STU_APPLICATION.APPLICATION_ID != 0 and STU_APPLICATION.APPLICATION_LINE_ID != 0
               JOIN ODS.UAC.VW_ALL_OFFER UAC_OFFER
                    ON UAC_OFFER.COURSE = STU_APPLICATION.ADMSN_CNTR_CRS_CD
                        AND UAC_OFFER.REFNUM = STU_APPLICATION.ADMSN_CNTR_APP_ID
               JOIN ODS.AMIS.S1STU_DET STU_DET
                    ON STU_DET.STU_ID = STU_APPLICATION.STU_ID
                        AND STU_DET.STU_CONSOL_FG = 'N'
               LEFT OUTER JOIN ODS.AMIS.S1SSP_STU_SPK CS_SSP
                               ON STU_APPLICATION.SPK_NO = CS_SSP.APPN_SPK_NO
                                   AND STU_APPLICATION.SPK_VER_NO = CS_SSP.APPN_VER_NO
                                   AND STU_APPLICATION.APPN_NO = CS_SSP.APPN_NO
                                   AND STU_APPLICATION.STU_ID = CS_SSP.STU_ID
               LEFT OUTER JOIN (
          SELECT UN_SSP.PARENT_SSP_NO,
                 MAX(UN_SSP.CENSUS_DT) UN_CENSUS_DT
          FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
                   JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
                        ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                            AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                            AND UN_SPK_DET.SPK_CAT_CD = 'UN'
          WHERE YEAR(UN_SSP.CENSUS_DT) > 1900
          GROUP BY UN_SSP.PARENT_SSP_NO
      ) UN_CENSUS
                               ON UN_CENSUS.PARENT_SSP_NO = CS_SSP.SSP_NO
     ) A
WHERE RN = 1
  AND NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.LNK_UAC_OFFER_TO_AMIS_COURSE_APPLICATION S
        WHERE S.HUB_COURSE_APPLICATION_KEY = MD5(IFNULL(A.STU_ID, '') || ',' ||
                                                 IFNULL(A.SPK_NO, 0) || ',' ||
                                                 IFNULL(A.SPK_VER_NO, 0) || ',' ||
                                                 IFNULL(CONCAT(A.APPLICATION_ID, '_', A.APPLICATION_LINE_ID), ''))
          AND S.HUB_UAC_OFFER_KEY = MD5(IFNULL(A.ROUNDNUM, 0) || ',' ||
                                        IFNULL(A.REFNUM, 0) || ',' ||
                                        IFNULL(A.COURSE, 0) || ',' ||
                                        A.YEAR || ',' ||
                                        A.SOURCE)
    );
    
    
    
    