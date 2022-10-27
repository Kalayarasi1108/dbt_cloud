INSERT INTO DATA_VAULT.CORE.SAT_STUDENT_UNIT_FEE_SUM (SAT_STUDENT_UNIT_FEE_SUM_SK, HUB_STUDENT_UNIT_FEE_KEY, SOURCE,
                                                      LOAD_DTS, ETL_JOB_ID, HASH_MD5, FEE_YEAR, FEE_SEQ_NO, EP_YEAR,
                                                      EP_NO, UNIT_SSP_NO, UNIT_SPK_CD, UNIT_SPK_VER_NO, UNIT_AVAIL_YEAR,
                                                      UNIT_STUDY_PERIOD_CODE, LOCATION_CODE, UNIT_AVAIL_KEY_NO,
                                                      UNIT_PARENT_SSP_NO, FEE_NAME, FEE_DESCRIPTION,
                                                      FINANCE_CATEGORY_NAME, FINANCE_CATEGORY_TYPE,
                                                      FEE_LIABILITY_NUMBER, UN_FEE_AMOUNT, DEFERRAL_FG,
                                                      FEE_DEBT_STATUS_CODE, FEE_DEBT_STATUS,
                                                      STU_FEES_FEE_OVERRIDE_AMOUNT, STU_FEES_ORIGINAL_FEE_AMOUNT,
                                                      STU_FEES_FIRST_DISCOUNT_AMOUNT,
                                                      STU_FEES_GOV_REPORT_ALLOCATED_AMOUNT, STU_FEES_FEE_AMOUNT,
                                                      STU_FEES_GOV_REPORT_FEE_AMOUNT, BALANCE_COST_CENTRE,
                                                      BALANCE_ACCOUNT_CODE, ACCOUNT_TRANSACTION_DATE,
                                                      ACCOUNT_TRANSACTION_TYPE_CODE,
                                                      ACCOUNT_TRANSACTION_TYPE_DESCRIPTION, CR_DR_INDICATOR,
                                                      LEDGER_CODE, ACCOUNT_CODE, FINANCE_PERIOD_CODE, DUE_DATE,
                                                      ACCOUNT_TRANSACTION_AMOUNT, ALLOCATED_AMOUNT, UNALLOCATED_AMOUNT,
                                                      RECEIPTED_AMOUNT, FIRST_DISCOUNT_AMOUNT,
                                                      INVOICE_TRANSACTION_AMOUNT, INVOICE_TRANSACTION_DISCOUNT_AMOUNT,
                                                      LAST_TRANSACTION_AMOUNT, INVOICE_TOTAL_AMOUNT,
                                                      ACCOUNT_TRANSACTION_STATUS_CODE, ACCOUNT_TRANSACTION_STATUS,
                                                      ACCOUNT_TRANSACTION_NUMBER,
                                                      IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_STUDENT_UNIT_FEE_SUM_SK,
       MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
           IFNULL(STU_FEES.EP_NO, 0) || ',' ||
           IFNULL(STU_FEES.FEE_SEQ_NO, 0)
           )                                     HUB_STUDENT_UNIT_FEE_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(
                   IFNULL(STU_FEES.FEE_YR, 0) || ',' ||
                   IFNULL(STU_FEES.FEE_SEQ_NO, 0) || ',' ||
                   IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
                   IFNULL(STU_FEES.EP_NO, 0) || ',' ||
                   IFNULL(STU_FEES.SSP_NO, 0) || ',' ||
                   IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                   IFNULL(UN_SSP.SPK_VER_NO, 0) || ',' ||
                   IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                   IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                   IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                   IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                   IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
                   IFNULL(FEE_DET.FEE_NAME, '') || ',' ||
                   IFNULL(FEE_DET.FEE_DESC, '') || ',' ||
                   IFNULL(FEE_DET.FIN_CAT_NAME, '') || ',' ||
                   IFNULL(FEE_DET.FIN_CAT_TYPE, '') || ',' ||
                   IFNULL(STU_FEES.FEE_LIAB_NO, 0) || ',' ||
                   IFNULL(STU_FEES.FEE_AMT, 0) || ',' ||
                   IFNULL(STU_FEES.DEFERRAL_FG, '') || ',' ||
                   IFNULL(STU_FEES.SSP_FEE_DBT_STTS, '') || ',' ||
                   IFNULL(FEE_DEBT_STTS_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(STU_FEES.FEE_OVR_AMT, 0) || ',' ||
                   IFNULL(STU_FEES.ORIG_FEE_AMT, 0) || ',' ||
                   IFNULL(STU_FEES.FIRST_DISC_AMT, 0) || ',' ||
                   IFNULL(STU_FEES.GV1_ALLOC_AMT, 0) || ',' ||
                   IFNULL(STU_FEES.FEE_AMT, 0) || ',' ||
                   IFNULL(STU_FEES.GV1_FEE_AMT, 0) || ',' ||
                   IFNULL(ACCT_TXN.BAL_COST_CTR, '') || ',' ||
                   IFNULL(ACCT_TXN.BAL_ACCOUNT_CD, '') || ',' ||
                   IFNULL(ACCT_TXN.ACCT_TXN_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(ACCT_TXN.ACCT_TXN_TYPE_CD, '') || ',' ||
                   IFNULL(FAT_TXN_TYPE.ACCT_TXN_TYPE_DESC, '') || ',' ||
                   IFNULL(FAT_TXN_TYPE.CR_DR_IND, '') || ',' ||
                   IFNULL(ACCT_TXN.LEDGER_CD, '') || ',' ||
                   IFNULL(ACCT_TXN.ACCOUNT_CD, '') || ',' ||
                   IFNULL(ACCT_TXN.FIN_PERIOD_CD, 0) || ',' ||
                   IFNULL(ACCT_TXN.DUE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(ACCT_TXN.ACCT_TXN_AMT, 0) || ',' ||
                   IFNULL(ACCT_TXN.ALLOC_AMT, 0) || ',' ||
                   IFNULL(ACCT_TXN.UNALLOC_AMT, 0) || ',' ||
                   IFNULL(ACCT_TXN.RECEIPTED_AMT, 0) || ',' ||
                   IFNULL(ACCT_TXN.FIRST_DISC_AMT, 0) || ',' ||
                   IFNULL(ACCT_TXN.INV_TXN_AMT, 0) || ',' ||
                   IFNULL(ACCT_TXN.INV_TXN_DISC_AMT, 0) || ',' ||
                   IFNULL(ACCT_TXN.LAST_TXN_AMT, 0) || ',' ||
                   IFNULL(ACCT_TXN.INV_TOTAL_AMT, 0) || ',' ||
                   IFNULL(ACCT_TXN.ACCT_TXN_STTS_CD, '') || ',' ||
                   IFNULL(ACCT_TXN_STTS_CD_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(ACCT_TXN.ACCT_TXN_NO, 0) || ',' ||
                   'N'
           )                                     HASH_MD5,
       STU_FEES.FEE_YR                           FEE_YEAR,
       STU_FEES.FEE_SEQ_NO                       FEE_SEQ_NO,
       STU_FEES.EP_YEAR,
       STU_FEES.EP_NO,
       STU_FEES.SSP_NO                           UNIT_SSP_NO,
       UN_SPK_DET.SPK_CD                         UNIT_SPK_CD,
       UN_SSP.SPK_VER_NO                         UNIT_SPK_VER_NO,
       UN_SSP.AVAIL_YR                           UNIT_AVAIL_YEAR,
       UN_SSP.SPRD_CD                            UNIT_STUDY_PERIOD_CODE,
       UN_SSP.LOCATION_CD                        LOCATION_CODE,
       UN_SSP.AVAIL_KEY_NO                       UNIT_AVAIL_KEY_NO,
       UN_SSP.PARENT_SSP_NO                      UNIT_PARENT_SSP_NO,
       FEE_DET.FEE_NAME,
       FEE_DET.FEE_DESC                          FEE_DESCRIPTION,
       FEE_DET.FIN_CAT_NAME                      FINANCE_CATEGORY_NAME,
       FEE_DET.FIN_CAT_TYPE                      FINANCE_CATEGORY_TYPE,
       STU_FEES.FEE_LIAB_NO                      FEE_LIABILITY_NUMBER,
       STU_FEES.FEE_AMT                          UN_FEE_AMOUNT,
       STU_FEES.DEFERRAL_FG,
       STU_FEES.SSP_FEE_DBT_STTS                 FEE_DEBT_STATUS_CODE,
       FEE_DEBT_STTS_CODE.CODE_DESCR             FEE_DEBT_STATUS,
       STU_FEES.FEE_OVR_AMT                      STU_FEES_FEE_OVERRIDE_AMOUNT,
       STU_FEES.ORIG_FEE_AMT                     STU_FEES_ORIGINAL_FEE_AMOUNT,
       STU_FEES.FIRST_DISC_AMT                   STU_FEES_FIRST_DISCOUNT_AMOUNT,
       STU_FEES.GV1_ALLOC_AMT                    STU_FEES_GOV_REPORT_ALLOCATED_AMOUNT,
       STU_FEES.FEE_AMT                          STU_FEES_FEE_AMOUNT,
       STU_FEES.GV1_FEE_AMT                      STU_FEES_GOV_REPORT_FEE_AMOUNT,
       ACCT_TXN.BAL_COST_CTR                     BALANCE_COST_CENTRE,
       ACCT_TXN.BAL_ACCOUNT_CD                   BALANCE_ACCOUNT_CODE,
       ACCT_TXN.ACCT_TXN_DT                      ACCOUNT_TRANSACTION_DATE,
       ACCT_TXN.ACCT_TXN_TYPE_CD                 ACCOUNT_TRANSACTION_TYPE_CODE,
       FAT_TXN_TYPE.ACCT_TXN_TYPE_DESC           ACCOUNT_TRANSACTION_TYPE_DESCRIPTION,
       FAT_TXN_TYPE.CR_DR_IND                    CR_DR_INDICATOR,
       ACCT_TXN.LEDGER_CD                        LEDGER_CODE,
       ACCT_TXN.ACCOUNT_CD                       ACCOUNT_CODE,
       ACCT_TXN.FIN_PERIOD_CD                    FINANCE_PERIOD_CODE,
       ACCT_TXN.DUE_DT                           DUE_DATE,
       ACCT_TXN.ACCT_TXN_AMT                     ACCOUNT_TRANSACTION_AMOUNT,
       ACCT_TXN.ALLOC_AMT                        ALLOCATED_AMOUNT,
       ACCT_TXN.UNALLOC_AMT                      UNALLOCATED_AMOUNT,
       ACCT_TXN.RECEIPTED_AMT                    RECEIPTED_AMOUNT,
       ACCT_TXN.FIRST_DISC_AMT                   FIRST_DISCOUNT_AMOUNT,
       ACCT_TXN.INV_TXN_AMT                      INVOICE_TRANSACTION_AMOUNT,
       ACCT_TXN.INV_TXN_DISC_AMT                 INVOICE_TRANSACTION_DISCOUNT_AMOUNT,
       ACCT_TXN.LAST_TXN_AMT                     LAST_TRANSACTION_AMOUNT,
       ACCT_TXN.INV_TOTAL_AMT                    INVOICE_TOTAL_AMOUNT,
       ACCT_TXN.ACCT_TXN_STTS_CD                 ACCOUNT_TRANSACTION_STATUS_CODE,
       ACCT_TXN_STTS_CD_CODE.CODE_DESCR          ACCOUNT_TRANSACTION_STATUS,
       ACCT_TXN.ACCT_TXN_NO                      ACCOUNT_TRANSACTION_NUMBER,
       'N'                                       IS_DELETED
FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
         JOIN ODS.AMIS.S1STU_FEES STU_FEES
              ON STU_FEES.SSP_NO = UN_SSP.SSP_NO
         JOIN ODS.AMIS.S1FLA_ACCT_TXN ACCT_TXN
              ON STU_FEES.FEE_SEQ_NO = ACCT_TXN.FEE_SEQ_NO
                  AND STU_FEES.FEE_TXN_NO = ACCT_TXN.ACCT_TXN_NO
         JOIN ODS.AMIS.S1FEE_DET FEE_DET
              ON FEE_DET.FEE_LIAB_NO = STU_FEES.FEE_LIAB_NO
                  AND FEE_DET.FEE_YR = STU_FEES.FEE_YR
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE FEE_GROUP_CODE
                         ON FEE_GROUP_CODE.CODE_ID = FEE_DET.FEE_GRP
                             AND FEE_GROUP_CODE.CODE_TYPE = 'FEE_GROUP'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE FEE_DEBT_STTS_CODE
                         ON FEE_DEBT_STTS_CODE.CODE_ID = STU_FEES.SSP_FEE_DBT_STTS
                             AND FEE_DEBT_STTS_CODE.CODE_TYPE = 'FEE_DEBT_STTS'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE ACCT_TXN_STTS_CD_CODE
                         ON ACCT_TXN_STTS_CD_CODE.CODE_ID = ACCT_TXN.ACCT_TXN_STTS_CD
                             AND ACCT_TXN_STTS_CD_CODE.CODE_TYPE = 'ACCT_TXN_STTS_CD'
         LEFT OUTER JOIN ODS.AMIS.S1FAT_TXN_TYPE FAT_TXN_TYPE
                         ON FAT_TXN_TYPE.ACCT_TXN_TYPE_CD = ACCT_TXN.ACCT_TXN_TYPE_CD

WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_STUDENT_UNIT_FEE_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_STUDENT_UNIT_FEE_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_STUDENT_UNIT_FEE_SUM) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_STUDENT_UNIT_FEE_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                               IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                                               IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
                                               IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
                                               IFNULL(STU_FEES.EP_NO, 0) || ',' ||
                                               IFNULL(STU_FEES.FEE_SEQ_NO, 0)
            )
          AND S.HASH_MD5 = MD5(
                    IFNULL(STU_FEES.FEE_YR, 0) || ',' ||
                    IFNULL(STU_FEES.FEE_SEQ_NO, 0) || ',' ||
                    IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
                    IFNULL(STU_FEES.EP_NO, 0) || ',' ||
                    IFNULL(STU_FEES.SSP_NO, 0) || ',' ||
                    IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(UN_SSP.SPK_VER_NO, 0) || ',' ||
                    IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                    IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                    IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                    IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
                    IFNULL(FEE_DET.FEE_NAME, '') || ',' ||
                    IFNULL(FEE_DET.FEE_DESC, '') || ',' ||
                    IFNULL(FEE_DET.FIN_CAT_NAME, '') || ',' ||
                    IFNULL(FEE_DET.FIN_CAT_TYPE, '') || ',' ||
                    IFNULL(STU_FEES.FEE_LIAB_NO, 0) || ',' ||
                    IFNULL(STU_FEES.FEE_AMT, 0) || ',' ||
                    IFNULL(STU_FEES.DEFERRAL_FG, '') || ',' ||
                    IFNULL(STU_FEES.SSP_FEE_DBT_STTS, '') || ',' ||
                    IFNULL(FEE_DEBT_STTS_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(STU_FEES.FEE_OVR_AMT, 0) || ',' ||
                    IFNULL(STU_FEES.ORIG_FEE_AMT, 0) || ',' ||
                    IFNULL(STU_FEES.FIRST_DISC_AMT, 0) || ',' ||
                    IFNULL(STU_FEES.GV1_ALLOC_AMT, 0) || ',' ||
                    IFNULL(STU_FEES.FEE_AMT, 0) || ',' ||
                    IFNULL(STU_FEES.GV1_FEE_AMT, 0) || ',' ||
                    IFNULL(ACCT_TXN.BAL_COST_CTR, '') || ',' ||
                    IFNULL(ACCT_TXN.BAL_ACCOUNT_CD, '') || ',' ||
                    IFNULL(ACCT_TXN.ACCT_TXN_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(ACCT_TXN.ACCT_TXN_TYPE_CD, '') || ',' ||
                    IFNULL(FAT_TXN_TYPE.ACCT_TXN_TYPE_DESC, '') || ',' ||
                    IFNULL(FAT_TXN_TYPE.CR_DR_IND, '') || ',' ||
                    IFNULL(ACCT_TXN.LEDGER_CD, '') || ',' ||
                    IFNULL(ACCT_TXN.ACCOUNT_CD, '') || ',' ||
                    IFNULL(ACCT_TXN.FIN_PERIOD_CD, 0) || ',' ||
                    IFNULL(ACCT_TXN.DUE_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(ACCT_TXN.ACCT_TXN_AMT, 0) || ',' ||
                    IFNULL(ACCT_TXN.ALLOC_AMT, 0) || ',' ||
                    IFNULL(ACCT_TXN.UNALLOC_AMT, 0) || ',' ||
                    IFNULL(ACCT_TXN.RECEIPTED_AMT, 0) || ',' ||
                    IFNULL(ACCT_TXN.FIRST_DISC_AMT, 0) || ',' ||
                    IFNULL(ACCT_TXN.INV_TXN_AMT, 0) || ',' ||
                    IFNULL(ACCT_TXN.INV_TXN_DISC_AMT, 0) || ',' ||
                    IFNULL(ACCT_TXN.LAST_TXN_AMT, 0) || ',' ||
                    IFNULL(ACCT_TXN.INV_TOTAL_AMT, 0) || ',' ||
                    IFNULL(ACCT_TXN.ACCT_TXN_STTS_CD, '') || ',' ||
                    IFNULL(ACCT_TXN_STTS_CD_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(ACCT_TXN.ACCT_TXN_NO, 0) || ',' ||
                    'N'
            )
    )
;