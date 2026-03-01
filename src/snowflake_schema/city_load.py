from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "CITY_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_CITY")
v.set("TMP_TABLE", "TMP_D_CITY")
v.set("TGT_TABLE", "TGT_D_CITY")
sf = Config(v)

truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

temp_query = f"""
    INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
    (CITY_NAME, POSTAL_CODE, STATE_NAME)
    SELECT DISTINCT
        CITY,
        POSTAL_CODE,
        STATE
    FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
"""
sf.execute_query(temp_query)

expire_query = f"""
    UPDATE {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
    SET
        TGT.IS_CURRENT = FALSE,
        TGT.EFFECTIVE_TO = CURRENT_TIMESTAMP()
    FROM (
        SELECT
            TMP.CITY_NAME,
            TMP.POSTAL_CODE,
            S.STATE_KEY
        FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
        JOIN {v.get('TGT_SCHEMA')}.TGT_D_STATE S
            ON TMP.STATE_NAME = S.STATE_NAME
           AND S.IS_CURRENT = TRUE
    ) AS SRC
    WHERE TGT.CITY_NAME = SRC.CITY_NAME
      AND TGT.POSTAL_CODE = SRC.POSTAL_CODE
      AND TGT.IS_CURRENT = TRUE
      AND TGT.STATE_KEY <> SRC.STATE_KEY;
"""
sf.execute_query(expire_query)

insert_query = f"""
    INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')}
    (
        CITY_NAME,
        POSTAL_CODE,
        STATE_KEY,
        IS_CURRENT,
        EFFECTIVE_FROM,
        EFFECTIVE_TO
    )
    SELECT
        SRC.CITY_NAME,
        SRC.POSTAL_CODE,
        SRC.STATE_KEY,
        TRUE,
        CURRENT_TIMESTAMP(),
        TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')
    FROM (
        SELECT
            TMP.CITY_NAME,
            TMP.POSTAL_CODE,
            S.STATE_KEY
        FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
        JOIN {v.get('TGT_SCHEMA')}.TGT_D_STATE S
            ON TMP.STATE_NAME = S.STATE_NAME
           AND S.IS_CURRENT = TRUE
    ) SRC
    LEFT JOIN {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} CUR
        ON CUR.CITY_NAME = SRC.CITY_NAME
       AND CUR.POSTAL_CODE = SRC.POSTAL_CODE
       AND CUR.STATE_KEY = SRC.STATE_KEY
       AND CUR.IS_CURRENT = TRUE
    WHERE CUR.CITY_KEY IS NULL;
"""
sf.execute_query(insert_query)

v.get('LOG').close()
