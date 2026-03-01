from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "SUBCATEGORY_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_SUBCATEGORY")
v.set("TMP_TABLE", "TMP_D_SUBCATEGORY")
v.set("TGT_TABLE", "TGT_D_SUBCATEGORY")
sf = Config(v)

# Truncate temporary table
truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

# Load to temporary table
temp_query = f"""
    INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
    (SUBCATEGORY_NAME, CATEGORY_NAME)
    SELECT DISTINCT
        SUB_CATEGORY,
        CATEGORY
    FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
"""
sf.execute_query(temp_query)

# Expire changed current records (SCD2)
expire_query = f"""
    UPDATE {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
    SET
        TGT.IS_CURRENT = FALSE,
        TGT.EFFECTIVE_TO = CURRENT_TIMESTAMP()
    FROM (
        SELECT
            TMP.SUBCATEGORY_NAME,
            C.CATEGORY_KEY
        FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
        JOIN {v.get('TGT_SCHEMA')}.TGT_D_CATEGORY C
            ON TMP.CATEGORY_NAME = C.CATEGORY_NAME
           AND C.IS_CURRENT = TRUE
    ) AS SRC
    WHERE TGT.SUBCATEGORY_NAME = SRC.SUBCATEGORY_NAME
      AND TGT.IS_CURRENT = TRUE
      AND TGT.CATEGORY_KEY <> SRC.CATEGORY_KEY;
"""
sf.execute_query(expire_query)

# Insert new/current records (new key or changed attributes)
insert_query = f"""
    INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')}
    (
        SUBCATEGORY_NAME,
        CATEGORY_KEY,
        IS_CURRENT,
        EFFECTIVE_FROM,
        EFFECTIVE_TO
    )
    SELECT
        SRC.SUBCATEGORY_NAME,
        SRC.CATEGORY_KEY,
        TRUE,
        CURRENT_TIMESTAMP(),
        TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')
    FROM (
        SELECT
            TMP.SUBCATEGORY_NAME,
            C.CATEGORY_KEY
        FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
        JOIN {v.get('TGT_SCHEMA')}.TGT_D_CATEGORY C
            ON TMP.CATEGORY_NAME = C.CATEGORY_NAME
           AND C.IS_CURRENT = TRUE
    ) SRC
    LEFT JOIN {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} CUR
        ON CUR.SUBCATEGORY_NAME = SRC.SUBCATEGORY_NAME
       AND CUR.IS_CURRENT = TRUE
       AND CUR.CATEGORY_KEY = SRC.CATEGORY_KEY
    WHERE CUR.SUBCATEGORY_KEY IS NULL;
"""
sf.execute_query(insert_query)

v.get('LOG').close()
