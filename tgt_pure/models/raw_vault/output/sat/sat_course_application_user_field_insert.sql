-- INSERT

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

SELECT DATA_VAULT.CORE.SEQ.nextval                             SAT_COURSE_APPLICATION_USER_FIELD_SK,
       MD5(IFNULL(St.STU_ID, '') || ',' ||
           IFNULL(St.SPK_NO, 0) || ',' ||
           IFNULL(St.SPK_VER_NO, 0) || ',' ||
           IFNULL(CONCAT(APP.APPLICATION_ID, '_', APP.APPLICATION_LINE_ID), '')) HUB_COURSE_APPLICATION_KEY,
       'AMIS'                                                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz                                          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                 ETL_JOB_ID,
       MD5(
                   IFNULL(St.STU_ID, '') || ',' ||
                   IFNULL(St.SPK_NO, 0) || ',' ||
                   IFNULL(St.SPK_VER_NO, 0) || ',' ||
                   IFNULL(CONCAT(APP.APPLICATION_ID, '_', APP.APPLICATION_LINE_ID), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA1, '') || ',' ||
                   IFNULL(APP.VAL_NUM1, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI1, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA2, '') || ',' ||
                   IFNULL(APP.VAL_NUM2, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI2, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA3, '') || ',' ||
                   IFNULL(APP.VAL_NUM3, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI3, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA4, '') || ',' ||
                   IFNULL(APP.VAL_NUM4, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI4, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA5, '') || ',' ||
                   IFNULL(APP.VAL_NUM5, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI5, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA6, '') || ',' ||
                   IFNULL(APP.VAL_NUM6, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI6, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA7, '') || ',' ||
                   IFNULL(APP.VAL_NUM7, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI7, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA8, '') || ',' ||
                   IFNULL(APP.VAL_NUM8, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI8, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA9, '') || ',' ||
                   IFNULL(APP.VAL_NUM9, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI9, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA10, '') || ',' ||
                   IFNULL(APP.VAL_NUM10, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI10, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA11, '') || ',' ||
                   IFNULL(APP.VAL_NUM11, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI11, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA12, '') || ',' ||
                   IFNULL(APP.VAL_NUM12, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI12, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA13, '') || ',' ||
                   IFNULL(APP.VAL_NUM13, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI13, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA14, '') || ',' ||
                   IFNULL(APP.VAL_NUM14, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI14, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA15, '') || ',' ||
                   IFNULL(APP.VAL_NUM15, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI15, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA16, '') || ',' ||
                   IFNULL(APP.VAL_NUM16, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI16, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA17, '') || ',' ||
                   IFNULL(APP.VAL_NUM17, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI17, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA18, '') || ',' ||
                   IFNULL(APP.VAL_NUM18, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI18, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA19, '') || ',' ||
                   IFNULL(APP.VAL_NUM19, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI19, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VAL_ALPHA20, '') || ',' ||
                   IFNULL(APP.VAL_NUM20, 0) || ',' ||
                   IFNULL(TO_CHAR(APP.VAL_DATEI20, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.VERS, 0) || ',' ||
                   IFNULL(APP.CRUSER, '') || ',' ||
                   IFNULL(TO_CHAR(APP.CRDATEI, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.CRTIMEI, 0) || ',' ||
                   IFNULL(APP.CRTERM, '') || ',' ||
                   IFNULL(APP.CRWINDOW, '') || ',' ||
                   IFNULL(APP.LAST_MOD_USER, '') || ',' ||
                   IFNULL(TO_CHAR(APP.LAST_MOD_DATEI, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(APP.LAST_MOD_TIMEI, 0) || ',' ||
                   IFNULL(APP.LAST_MOD_TERM, '') || ',' ||
                   IFNULL(APP.LAST_MOD_WINDOW, '') || ',' ||
                   IFNULL(NULL, '') || ',' ||
                   IFNULL(NULL, '') || ',' ||
                   IFNULL(NULL, '') || ',' ||
                   IFNULL(NULL, '') || ',' ||
                   IFNULL(NULL, 0) || ',' ||
                   IFNULL(NULL, 0) || ',' ||
                   IFNULL(NULL, 0) || ',' ||
                   IFNULL(NULL, 0) || ',' ||
                   IFNULL('N', '')
           )                                                                     HASH_MD5,
       STU_ID,
       SPK_NO,
       SPK_VER_NO,
       CONCAT(APP.APPLICATION_ID, '_', APP.APPLICATION_LINE_ID) as               APPN_NO,
       VAL_ALPHA1,
       VAL_NUM1,
       VAL_DATEI1,
       VAL_ALPHA2,
       VAL_NUM2,
       VAL_DATEI2,
       VAL_ALPHA3,
       VAL_NUM3,
       VAL_DATEI3,
       VAL_ALPHA4,
       VAL_NUM4,
       VAL_DATEI4,
       VAL_ALPHA5,
       VAL_NUM5,
       VAL_DATEI5,
       VAL_ALPHA6,
       VAL_NUM6,
       VAL_DATEI6,
       VAL_ALPHA7,
       VAL_NUM7,
       VAL_DATEI7,
       VAL_ALPHA8,
       VAL_NUM8,
       VAL_DATEI8,
       VAL_ALPHA9,
       VAL_NUM9,
       VAL_DATEI9,
       VAL_ALPHA10,
       VAL_NUM10,
       VAL_DATEI10,
       VAL_ALPHA11,
       VAL_NUM11,
       VAL_DATEI11,
       VAL_ALPHA12,
       VAL_NUM12,
       VAL_DATEI12,
       VAL_ALPHA13,
       VAL_NUM13,
       VAL_DATEI13,
       VAL_ALPHA14,
       VAL_NUM14,
       VAL_DATEI14,
       VAL_ALPHA15,
       VAL_NUM15,
       VAL_DATEI15,
       VAL_ALPHA16,
       VAL_NUM16,
       VAL_DATEI16,
       VAL_ALPHA17,
       VAL_NUM17,
       VAL_DATEI17,
       VAL_ALPHA18,
       VAL_NUM18,
       VAL_DATEI18,
       VAL_ALPHA19,
       VAL_NUM19,
       VAL_DATEI19,
       VAL_ALPHA20,
       VAL_NUM20,
       VAL_DATEI20,
       APP.VERS,
       APP.CRUSER,
       APP.CRDATEI,
       APP.CRTIMEI,
       APP.CRTERM,
       APP.CRWINDOW,
       APP.LAST_MOD_USER,
       APP.LAST_MOD_DATEI,
       APP.LAST_MOD_TIMEI,
       APP.LAST_MOD_TERM,
       APP.LAST_MOD_WINDOW,
       NULL                                                     as               TECHONE_FLD1,
       NULL                                                     as               TECHONE_FLD2,
       NULL                                                     as               TECHONE_FLD3,
       NULL                                                     as               TECHONE_FLD4,
       NULL                                                     as               TECHONE_FLD5,
       NULL                                                     as               TECHONE_FLD6,
       NULL                                                     as               TECHONE_FLD7,
       NULL                                                     as               TECHONE_FLD8,
       'N'                                                                       IS_DELETED
FROM ODS.AMIS.S1APP_APPLICATION_LNE_UFLD APP
         INNER JOIN ODS.AMIS.S1APP_STUDY as St on APP.APPLICATION_ID = St.Application_id
    and App.application_line_id = st.application_line_id
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_COURSE_APPLICATION_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS)
                          OVER (PARTITION BY HUB_COURSE_APPLICATION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_COURSE_APPLICATION_USER_FIELD) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_COURSE_APPLICATION_KEY = MD5(IFNULL(St.STU_ID, '') || ',' ||
                                                 IFNULL(St.SPK_NO, 0) || ',' ||
                                                 IFNULL(St.SPK_VER_NO, 0) || ',' ||
                                                 IFNULL(CONCAT(APP.APPLICATION_ID, '_', APP.APPLICATION_LINE_ID), ''))
          AND S.HASH_MD5 = MD5(
                    IFNULL(St.STU_ID, '') || ',' ||
                    IFNULL(St.SPK_NO, 0) || ',' ||
                    IFNULL(St.SPK_VER_NO, 0) || ',' ||
                    IFNULL(CONCAT(APP.APPLICATION_ID, '_', APP.APPLICATION_LINE_ID), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA1, '') || ',' ||
                    IFNULL(APP.VAL_NUM1, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI1, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA2, '') || ',' ||
                    IFNULL(APP.VAL_NUM2, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI2, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA3, '') || ',' ||
                    IFNULL(APP.VAL_NUM3, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI3, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA4, '') || ',' ||
                    IFNULL(APP.VAL_NUM4, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI4, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA5, '') || ',' ||
                    IFNULL(APP.VAL_NUM5, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI5, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA6, '') || ',' ||
                    IFNULL(APP.VAL_NUM6, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI6, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA7, '') || ',' ||
                    IFNULL(APP.VAL_NUM7, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI7, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA8, '') || ',' ||
                    IFNULL(APP.VAL_NUM8, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI8, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA9, '') || ',' ||
                    IFNULL(APP.VAL_NUM9, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI9, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA10, '') || ',' ||
                    IFNULL(APP.VAL_NUM10, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI10, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA11, '') || ',' ||
                    IFNULL(APP.VAL_NUM11, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI11, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA12, '') || ',' ||
                    IFNULL(APP.VAL_NUM12, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI12, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA13, '') || ',' ||
                    IFNULL(APP.VAL_NUM13, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI13, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA14, '') || ',' ||
                    IFNULL(APP.VAL_NUM14, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI14, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA15, '') || ',' ||
                    IFNULL(APP.VAL_NUM15, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI15, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA16, '') || ',' ||
                    IFNULL(APP.VAL_NUM16, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI16, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA17, '') || ',' ||
                    IFNULL(APP.VAL_NUM17, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI17, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA18, '') || ',' ||
                    IFNULL(APP.VAL_NUM18, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI18, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA19, '') || ',' ||
                    IFNULL(APP.VAL_NUM19, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI19, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VAL_ALPHA20, '') || ',' ||
                    IFNULL(APP.VAL_NUM20, 0) || ',' ||
                    IFNULL(TO_CHAR(APP.VAL_DATEI20, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.VERS, 0) || ',' ||
                    IFNULL(APP.CRUSER, '') || ',' ||
                    IFNULL(TO_CHAR(APP.CRDATEI, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.CRTIMEI, 0) || ',' ||
                    IFNULL(APP.CRTERM, '') || ',' ||
                    IFNULL(APP.CRWINDOW, '') || ',' ||
                    IFNULL(APP.LAST_MOD_USER, '') || ',' ||
                    IFNULL(TO_CHAR(APP.LAST_MOD_DATEI, 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(APP.LAST_MOD_TIMEI, 0) || ',' ||
                    IFNULL(APP.LAST_MOD_TERM, '') || ',' ||
                    IFNULL(APP.LAST_MOD_WINDOW, '') || ',' ||
                    IFNULL(NULL, '') || ',' ||
                    IFNULL(NULL, '') || ',' ||
                    IFNULL(NULL, '') || ',' ||
                    IFNULL(NULL, '') || ',' ||
                    IFNULL(NULL, 0) || ',' ||
                    IFNULL(NULL, 0) || ',' ||
                    IFNULL(NULL, 0) || ',' ||
                    IFNULL(NULL, 0) || ',' ||
                    IFNULL('N', '')
            )
    )