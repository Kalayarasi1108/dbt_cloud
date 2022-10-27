INSERT INTO DATA_VAULT.CORE.SAT_UAC_COURSE(SAT_UAC_COURSE_SK, HUB_UAC_COURSE_KEY, SOURCE, LOAD_DTS, ETL_JOB_ID,
                                           HASH_MD5, YEAR, COURSE, COURSEP, CATEGORY, CSSTATUS, TYPE, INSTCODE,
                                           CAMPCODE, GCODE, CSTITLE, CSDESC, FTTYPE, PTTYPE, EXTMODE, INTMODE, ONLMODE,
                                           PARTNER, OPENY12, OPENOS, OPENLOC, CSLEVEL, FOSCD, FOSSUP, CAMPLOC, AOUCODE,
                                           SPCSTYPE, ABSMIN, LOADMARG, PRQWAIVE, ESNOTED, ESSTALE, LSNOTED, LSSTALE,
                                           NSNOTED, PSNOTED, XSNOTED, TMAFLAG, COMBFLAG, SELFLAG, COMMDATE, ENROLDATE,
                                           DATENEW, DATEDOC, DATECOP, MAGEDATE, IS_DELETED)

WITH SAT_UAC_COURSE AS (
    SELECT HUB_UAC_COURSE_KEY,
           HASH_MD5,
           SOURCE,
           LOAD_DTS,
           LEAD(LOAD_DTS)
                OVER (PARTITION BY HUB_UAC_COURSE_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
    FROM DATA_VAULT.CORE.SAT_UAC_COURSE
)
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_UAC_COURSE_SK,
       MD5(
                   IFNULL(CRS.COURSE, 0) || ',' ||
                   CRS.YEAR || ',' ||
                   CRS.SOURCE
           )                                     HUB_UAC_COURSE_KEY,
       CRS.SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(
                   IFNULL(CRS.YEAR, '') || ',' ||
                   IFNULL(CRS.COURSE, 0) || ',' ||
                   IFNULL(CRS.COURSEP, '') || ',' ||
                   IFNULL(CRS.CATEGORY, '') || ',' ||
                   IFNULL(CRS.CSSTATUS, '') || ',' ||
                   IFNULL(CRS.TYPE, '') || ',' ||
                   IFNULL(CRS.INSTCODE, '') || ',' ||
                   IFNULL(CRS.CAMPCODE, '') || ',' ||
                   IFNULL(CRS.GCODE, '') || ',' ||
                   IFNULL(CRS.CSTITLE, '') || ',' ||
                   IFNULL(CRS.CSDESC, '') || ',' ||
                   IFNULL(CRS.FTTYPE, '') || ',' ||
                   IFNULL(CRS.PTTYPE, '') || ',' ||
                   IFNULL(CRS.EXTMODE, '') || ',' ||
                   IFNULL(CRS.INTMODE, '') || ',' ||
                   IFNULL(CRS.ONLMODE, '') || ',' ||
                   IFNULL(CRS.PARTNER, '') || ',' ||
                   IFNULL(CRS.OPENY12, '') || ',' ||
                   IFNULL(CRS.OPENOS, '') || ',' ||
                   IFNULL(CRS.OPENLOC, '') || ',' ||
                   IFNULL(CRS.CSLEVEL, '') || ',' ||
                   IFNULL(CRS.FOSCD, 0) || ',' ||
                   IFNULL(CRS.FOSSUP, 0) || ',' ||
                   IFNULL(CRS.CAMPLOC, '') || ',' ||
                   IFNULL(CRS.AOUCODE, '') || ',' ||
                   IFNULL(CRS.SPCSTYPE, '') || ',' ||
                   IFNULL(CRS.ABSMIN, 0) || ',' ||
                   IFNULL(CRS.LOADMARG, 0) || ',' ||
                   IFNULL(CRS.PRQWAIVE, 0) || ',' ||
                   IFNULL(CRS.ESNOTED, '') || ',' ||
                   IFNULL(CRS.ESSTALE, 0) || ',' ||
                   IFNULL(CRS.LSNOTED, '') || ',' ||
                   IFNULL(CRS.LSSTALE, 0) || ',' ||
                   IFNULL(CRS.NSNOTED, '') || ',' ||
                   IFNULL(CRS.PSNOTED, '') || ',' ||
                   IFNULL(CRS.XSNOTED, '') || ',' ||
                   IFNULL(CRS.TMAFLAG, '') || ',' ||
                   IFNULL(CRS.COMBFLAG, '') || ',' ||
                   IFNULL(CRS.SELFLAG, '') || ',' ||
                   IFNULL(TO_CHAR(TO_DATE(CRS.COMMDATE), 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(TO_CHAR(TO_DATE(CRS.ENROLDATE), 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(TO_CHAR(TO_DATE(CRS.DATENEW), 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(TO_CHAR(TO_DATE(CRS.DATEDOC), 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(TO_CHAR(TO_DATE(CRS.DATECOP), 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(TO_CHAR(TO_DATE(CRS.MAGEDATE), 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL('N', '')
           )                                     HASH_MD5,
       CRS.YEAR,
       CRS.COURSE,
       CRS.COURSEP,
       CRS.CATEGORY,
       CRS.CSSTATUS,
       CRS.TYPE,
       CRS.INSTCODE,
       CRS.CAMPCODE,
       CRS.GCODE,
       CRS.CSTITLE,
       CRS.CSDESC,
       CRS.FTTYPE,
       CRS.PTTYPE,
       CRS.EXTMODE,
       CRS.INTMODE,
       CRS.ONLMODE,
       CRS.PARTNER,
       CRS.OPENY12,
       CRS.OPENOS,
       CRS.OPENLOC,
       CRS.CSLEVEL,
       CRS.FOSCD,
       CRS.FOSSUP,
       CRS.CAMPLOC,
       CRS.AOUCODE,
       CRS.SPCSTYPE,
       CRS.ABSMIN,
       CRS.LOADMARG,
       CRS.PRQWAIVE,
       CRS.ESNOTED,
       CRS.ESSTALE,
       CRS.LSNOTED,
       CRS.LSSTALE,
       CRS.NSNOTED,
       CRS.PSNOTED,
       CRS.XSNOTED,
       CRS.TMAFLAG,
       CRS.COMBFLAG,
       CRS.SELFLAG,
       TO_DATE(CRS.COMMDATE)                     COMMDATE,
       TO_DATE(CRS.ENROLDATE)                    ENROLDATE,
       TO_DATE(CRS.DATENEW)                      DATENEW,
       TO_DATE(CRS.DATEDOC)                      DATEDOC,
       TO_DATE(CRS.DATECOP)                      DATECOP,
       TO_DATE(CRS.MAGEDATE)                     MAGEDATE,
       'N'                                       IS_DELETED
FROM ODS.UAC.VW_ALL_COURSE CRS
         JOIN DATA_VAULT.CORE.HUB_UAC_COURSE H ON H.HUB_UAC_COURSE_KEY = MD5(
            IFNULL(CRS.COURSE, 0) || ',' ||
            CRS.YEAR || ',' ||
            CRS.SOURCE
    )

WHERE NOT EXISTS
    (
        SELECT NULL
        FROM SAT_UAC_COURSE SAT
        WHERE SAT.EFFECTIVE_END_DTS IS NULL
          AND SAT.SOURCE = CRS.SOURCE
          AND SAT.HUB_UAC_COURSE_KEY = MD5(
                    IFNULL(CRS.COURSE, 0) || ',' ||
                    CRS.YEAR || ',' ||
                    CRS.SOURCE
            )
          AND SAT.HASH_MD5 = MD5(
                    IFNULL(CRS.YEAR, '') || ',' ||
                    IFNULL(CRS.COURSE, 0) || ',' ||
                    IFNULL(CRS.COURSEP, '') || ',' ||
                    IFNULL(CRS.CATEGORY, '') || ',' ||
                    IFNULL(CRS.CSSTATUS, '') || ',' ||
                    IFNULL(CRS.TYPE, '') || ',' ||
                    IFNULL(CRS.INSTCODE, '') || ',' ||
                    IFNULL(CRS.CAMPCODE, '') || ',' ||
                    IFNULL(CRS.GCODE, '') || ',' ||
                    IFNULL(CRS.CSTITLE, '') || ',' ||
                    IFNULL(CRS.CSDESC, '') || ',' ||
                    IFNULL(CRS.FTTYPE, '') || ',' ||
                    IFNULL(CRS.PTTYPE, '') || ',' ||
                    IFNULL(CRS.EXTMODE, '') || ',' ||
                    IFNULL(CRS.INTMODE, '') || ',' ||
                    IFNULL(CRS.ONLMODE, '') || ',' ||
                    IFNULL(CRS.PARTNER, '') || ',' ||
                    IFNULL(CRS.OPENY12, '') || ',' ||
                    IFNULL(CRS.OPENOS, '') || ',' ||
                    IFNULL(CRS.OPENLOC, '') || ',' ||
                    IFNULL(CRS.CSLEVEL, '') || ',' ||
                    IFNULL(CRS.FOSCD, 0) || ',' ||
                    IFNULL(CRS.FOSSUP, 0) || ',' ||
                    IFNULL(CRS.CAMPLOC, '') || ',' ||
                    IFNULL(CRS.AOUCODE, '') || ',' ||
                    IFNULL(CRS.SPCSTYPE, '') || ',' ||
                    IFNULL(CRS.ABSMIN, 0) || ',' ||
                    IFNULL(CRS.LOADMARG, 0) || ',' ||
                    IFNULL(CRS.PRQWAIVE, 0) || ',' ||
                    IFNULL(CRS.ESNOTED, '') || ',' ||
                    IFNULL(CRS.ESSTALE, 0) || ',' ||
                    IFNULL(CRS.LSNOTED, '') || ',' ||
                    IFNULL(CRS.LSSTALE, 0) || ',' ||
                    IFNULL(CRS.NSNOTED, '') || ',' ||
                    IFNULL(CRS.PSNOTED, '') || ',' ||
                    IFNULL(CRS.XSNOTED, '') || ',' ||
                    IFNULL(CRS.TMAFLAG, '') || ',' ||
                    IFNULL(CRS.COMBFLAG, '') || ',' ||
                    IFNULL(CRS.SELFLAG, '') || ',' ||
                    IFNULL(TO_CHAR(TO_DATE(CRS.COMMDATE), 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(TO_CHAR(TO_DATE(CRS.ENROLDATE), 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(TO_CHAR(TO_DATE(CRS.DATENEW), 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(TO_CHAR(TO_DATE(CRS.DATEDOC), 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(TO_CHAR(TO_DATE(CRS.DATECOP), 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL(TO_CHAR(TO_DATE(CRS.MAGEDATE), 'YYYY-MM-DD'), '') || ',' ||
                    IFNULL('N', '')
            )
    );
