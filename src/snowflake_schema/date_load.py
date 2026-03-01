# from Config import Config
# from Logger import Logger
# from Variable import Variables

# v = Variables()
# v.set("SCRIPT_NAME", "DATE_LOAD")
# v.set("LOG", Logger(v))
# sf = Config(v)

# # clear existing dates (optional)
# sf.execute_query(f"TRUNCATE TABLE {v.get('TGT_SCHEMA')}.TGT_D_DATE")

# # populate calendar from 2022‑01‑01 through 2026‑12‑31
# sf.execute_query(f"""
#     INSERT INTO {v.get('TGT_SCHEMA')}.TGT_D_DATE
#     SELECT
#         TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))                                     AS DATE_KEY,
#         d                                                                       AS FULL_DATE,
#         YEAR(d)                                                                 AS YEAR,
#         QUARTER(d)                                                              AS QUARTER,
#         MONTH(d)                                                                AS MONTH,
#         TO_CHAR(d, 'Mon')                                                       AS MONTH_NAME,
#         WEEK(d)                                                                 AS WEEK,
#         DAY(d)                                                                  AS DAY,
#         DAYOFWEEK(d)                                                            AS DAY_OF_WEEK,
#         TO_CHAR(d, 'Dy')                                                        AS DAY_NAME,
#         IFF(DAYOFWEEK(d) IN (0, 6), TRUE, FALSE)                               AS IS_WEEKEND
#     FROM (
#         SELECT DATEADD(DAY, SEQ4(), '2022-01-01'::DATE) AS d
#         FROM TABLE(GENERATOR(ROWCOUNT => 4000))
#     )
#     WHERE d <= '2026-12-31'::DATE;
# """ )

# v.get('LOG').close()
