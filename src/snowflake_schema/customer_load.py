from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "CUSTOMER_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_CUSTOMER")
v.set("TMP_TABLE", "TMP_D_CUSTOMER_SF")
v.set("TGT_TABLE", "TGT_D_CUSTOMER_SF")
sf = Config(v)

# Truncate temporary table
truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

temp_query = f"""
    INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
    (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT_NAME)
    SELECT DISTINCT
        CUSTOMER_ID,
        CUSTOMER_NAME,
        SEGMENT
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
            TMP.CUSTOMER_ID,
            TMP.CUSTOMER_NAME,
            S.SEGMENT_KEY
        FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
        JOIN {v.get('TGT_SCHEMA')}.TGT_D_SEGMENT S
            ON TMP.SEGMENT_NAME = S.SEGMENT_NAME
           AND S.IS_CURRENT = TRUE
    ) AS SRC
    WHERE TGT.CUSTOMER_ID = SRC.CUSTOMER_ID
      AND TGT.IS_CURRENT = TRUE
      AND (
          TGT.CUSTOMER_NAME <> SRC.CUSTOMER_NAME
          OR TGT.SEGMENT_KEY <> SRC.SEGMENT_KEY
      );
"""
sf.execute_query(expire_query)

insert_query = f"""
    INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')}
    (
        CUSTOMER_ID,
        CUSTOMER_NAME,
        SEGMENT_KEY,
        IS_CURRENT,
        EFFECTIVE_FROM,
        EFFECTIVE_TO
    )
    SELECT
        SRC.CUSTOMER_ID,
        SRC.CUSTOMER_NAME,
        SRC.SEGMENT_KEY,
        TRUE,
        CURRENT_TIMESTAMP(),
        TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')
    FROM (
        SELECT
            TMP.CUSTOMER_ID,
            TMP.CUSTOMER_NAME,
            S.SEGMENT_KEY
        FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
        JOIN {v.get('TGT_SCHEMA')}.TGT_D_SEGMENT S
            ON TMP.SEGMENT_NAME = S.SEGMENT_NAME
           AND S.IS_CURRENT = TRUE
    ) SRC
    LEFT JOIN {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} CUR
        ON CUR.CUSTOMER_ID = SRC.CUSTOMER_ID
       AND CUR.CUSTOMER_NAME = SRC.CUSTOMER_NAME
       AND CUR.SEGMENT_KEY = SRC.SEGMENT_KEY
       AND CUR.IS_CURRENT = TRUE
    WHERE CUR.CUSTOMER_KEY IS NULL;
"""
sf.execute_query(insert_query)

v.get('LOG').close()
