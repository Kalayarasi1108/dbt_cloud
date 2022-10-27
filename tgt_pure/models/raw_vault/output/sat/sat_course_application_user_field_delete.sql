-- DELETE

INSERT INTO DATA_VAULT.CORE.SAT_COURSE_APPLICATION_USER_FIELD (SAT_COURSE_APPLICATION_USER_FIELD_SK,
                                                                              HUB_COURSE_APPLICATION_KEY, SOURCE,
                                                                              LOAD_DTS, ETL_JOB_ID,
                                                                              HASH_MD5, STU_ID, SPK_NO, SPK_VER_NO,
                                                                              APPN_NO,
                                                                              VAL_ALPHA1, VAL_NUM1, VAL_DATEI1,
                                                                              VAL_ALPHA2, VAL_NUM2,
                                                                              VAL_DATEI2, VAL_ALPHA3, VAL_NUM3,
                                                                              VAL_DATEI3, VAL_ALPHA4,
                                                                              VAL_NUM4, VAL_DATEI4, VAL_ALPHA5,
                                                                              VAL_NUM5, VAL_DATEI5,
                                                                              VAL_ALPHA6, VAL_NUM6, VAL_DATEI6,
                                                                              VAL_ALPHA7, VAL_NUM7,
                                                                              VAL_DATEI7, VAL_ALPHA8, VAL_NUM8,
                                                                              VAL_DATEI8, VAL_ALPHA9,
                                                                              VAL_NUM9, VAL_DATEI9, VAL_ALPHA10,
                                                                              VAL_NUM10,
                                                                              VAL_DATEI10, VAL_ALPHA11, VAL_NUM11,
                                                                              VAL_DATEI11,
                                                                              VAL_ALPHA12, VAL_NUM12, VAL_DATEI12,
                                                                              VAL_ALPHA13,
                                                                              VAL_NUM13, VAL_DATEI13, VAL_ALPHA14,
                                                                              VAL_NUM14,
                                                                              VAL_DATEI14, VAL_ALPHA15, VAL_NUM15,
                                                                              VAL_DATEI15,
                                                                              VAL_ALPHA16, VAL_NUM16, VAL_DATEI16,
                                                                              VAL_ALPHA17,
                                                                              VAL_NUM17, VAL_DATEI17, VAL_ALPHA18,
                                                                              VAL_NUM18,
                                                                              VAL_DATEI18, VAL_ALPHA19, VAL_NUM19,
                                                                              VAL_DATEI19,
                                                                              VAL_ALPHA20, VAL_NUM20, VAL_DATEI20, VERS,
                                                                              CRUSER,
                                                                              CRDATEI, CRTIMEI, CRTERM, CRWINDOW,
                                                                              LAST_MOD_USER,
                                                                              LAST_MOD_DATEI, LAST_MOD_TIMEI,
                                                                              LAST_MOD_TERM,
                                                                              LAST_MOD_WINDOW, TECHONE_FLD1,
                                                                              TECHONE_FLD2,
                                                                              TECHONE_FLD3, TECHONE_FLD4, TECHONE_FLD5,
                                                                              TECHONE_FLD6,
                                                                              TECHONE_FLD7, TECHONE_FLD8, IS_DELETED)


SELECT DATA_VAULT.CORE.SEQ.nextval SAT_COURSE_APPLICATION_USER_FIELD_SK,
       MD5(S.HUB_COURSE_APPLICATION_KEY)             HUB_COURSE_APPLICATION_KEY,
       'AMIS'                                        SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ              LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ     ETL_JOB_ID,
       MD5('')                                       HASH_MD5,
       NULL                                          STU_ID,
       NULL                                          SPK_NO,
       NULL                                          SPK_VER_NO,
       NULL                                          APPN_NO,
       NULL                                          VAL_ALPHA1,
       NULL                                          VAL_NUM1,
       NULL                                          VAL_DATEI1,
       NULL                                          VAL_ALPHA2,
       NULL                                          VAL_NUM2,
       NULL                                          VAL_DATEI2,
       NULL                                          VAL_ALPHA3,
       NULL                                          VAL_NUM3,
       NULL                                          VAL_DATEI3,
       NULL                                          VAL_ALPHA4,
       NULL                                          VAL_NUM4,
       NULL                                          VAL_DATEI4,
       NULL                                          VAL_ALPHA5,
       NULL                                          VAL_NUM5,
       NULL                                          VAL_DATEI5,
       NULL                                          VAL_ALPHA6,
       NULL                                          VAL_NUM6,
       NULL                                          VAL_DATEI6,
       NULL                                          VAL_ALPHA7,
       NULL                                          VAL_NUM7,
       NULL                                          VAL_DATEI7,
       NULL                                          VAL_ALPHA8,
       NULL                                          VAL_NUM8,
       NULL                                          VAL_DATEI8,
       NULL                                          VAL_ALPHA9,
       NULL                                          VAL_NUM9,
       NULL                                          VAL_DATEI9,
       NULL                                          VAL_ALPHA10,
       NULL                                          VAL_NUM10,
       NULL                                          VAL_DATEI10,
       NULL                                          VAL_ALPHA11,
       NULL                                          VAL_NUM11,
       NULL                                          VAL_DATEI11,
       NULL                                          VAL_ALPHA12,
       NULL                                          VAL_NUM12,
       NULL                                          VAL_DATEI12,
       NULL                                          VAL_ALPHA13,
       NULL                                          VAL_NUM13,
       NULL                                          VAL_DATEI13,
       NULL                                          VAL_ALPHA14,
       NULL                                          VAL_NUM14,
       NULL                                          VAL_DATEI14,
       NULL                                          VAL_ALPHA15,
       NULL                                          VAL_NUM15,
       NULL                                          VAL_DATEI15,
       NULL                                          VAL_ALPHA16,
       NULL                                          VAL_NUM16,
       NULL                                          VAL_DATEI16,
       NULL                                          VAL_ALPHA17,
       NULL                                          VAL_NUM17,
       NULL                                          VAL_DATEI17,
       NULL                                          VAL_ALPHA18,
       NULL                                          VAL_NUM18,
       NULL                                          VAL_DATEI18,
       NULL                                          VAL_ALPHA19,
       NULL                                          VAL_NUM19,
       NULL                                          VAL_DATEI19,
       NULL                                          VAL_ALPHA20,
       NULL                                          VAL_NUM20,
       NULL                                          VAL_DATEI20,
       NULL                                          VERS,
       NULL                                          CRUSER,
       NULL                                          CRDATEI,
       NULL                                          CRTIMEI,
       NULL                                          CRTERM,
       NULL                                          CRWINDOW,
       NULL                                          LAST_MOD_USER,
       NULL                                          LAST_MOD_DATEI,
       NULL                                          LAST_MOD_TIMEI,
       NULL                                          LAST_MOD_TERM,
       NULL                                          LAST_MOD_WINDOW,
       NULL                                          TECHONE_FLD1,
       NULL                                          TECHONE_FLD2,
       NULL                                          TECHONE_FLD3,
       NULL                                          TECHONE_FLD4,
       NULL                                          TECHONE_FLD5,
       NULL                                          TECHONE_FLD6,
       NULL                                          TECHONE_FLD7,
       NULL                                          TECHONE_FLD8,
       'Y'                                           IS_DELETED
FROM (
         SELECT HUB.STU_ID,
                HUB.SPK_NO,
                HUB.SPK_VER_NO,
                HUB.APPN_NO::varchar(100) as                                                      APPN_NO,
                SAT.HUB_COURSE_APPLICATION_KEY,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_COURSE_APPLICATION_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_COURSE_APPLICATION_USER_FIELD SAT
                  JOIN DATA_VAULT.CORE.HUB_COURSE_APPLICATION HUB
                       ON HUB.HUB_COURSE_APPLICATION_KEY = SAT.HUB_COURSE_APPLICATION_KEY AND HUB.SOURCE = 'AMIS'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'

  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1STU_APP_UFLD_DTL APP
        WHERE APP.STU_ID = S.STU_ID
          AND APP.SPK_NO = S.SPK_NO
          AND APP.SPK_VER_NO = S.SPK_VER_NO
          AND APP.APPN_NO::varchar(100) = S.APPN_NO
        UNION ALL
        SELECT NULL
        FROM ODS.AMIS.S1APP_APPLICATION_LNE_UFLD APPL
                 INNER JOIN
             ODS.AMIS.S1APP_APPLICATION as a on a.application_id = appl.application_id
                 INNER JOIN ODS.AMIS.S1APP_APPLICATION_LINE as l on a.application_id = l.application_id
                 INNER JOIN ODS.AMIS.S1APP_STUDY as st on l.application_id = st.application_id
            and st.application_line_id = l.application_line_id
                 LEFT JOIN ODS.AMIS.S1APP_OFFER as o on o.application_id = l.application_id
            and l.application_line_id = o.application_line_id

        WHERE st.STU_ID = S.STU_ID
          AND st.SPK_NO = S.SPK_NO
          AND st.SPK_VER_NO = S.SPK_VER_NO
          AND CONCAT(APPL.APPLICATION_ID, '_', APPL.APPLICATION_LINE_ID) = S.APPN_NO
    );