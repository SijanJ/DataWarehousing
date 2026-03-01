from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "CATEGORY_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_CATEGORY")
v.set("TMP_TABLE", "TMP_D_CATEGORY")
v.set("TGT_TABLE", "TGT_D_CATEGORY")
sf = Config(v)

truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

temp_query = f"""
    INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
    (CATEGORY_NAME)
    SELECT DISTINCT
        CATEGORY
    FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
"""
sf.execute_query(temp_query)

sf.execute_query(f"""
    UPDATE {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
    SET
        IS_CURRENT = FALSE,
        EFFECTIVE_TO = CURRENT_TIMESTAMP()
    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
    WHERE TGT.CATEGORY_NAME = TMP.CATEGORY_NAME
      AND TGT.IS_CURRENT = TRUE
      AND TGT.CATEGORY_NAME <> TMP.CATEGORY_NAME
""")

sf.execute_query(f"""
    INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} (CATEGORY_NAME, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
    SELECT TMP.CATEGORY_NAME, TRUE, CURRENT_TIMESTAMP(), TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')
    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
    LEFT JOIN {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} CUR
        ON CUR.CATEGORY_NAME = TMP.CATEGORY_NAME
       AND CUR.IS_CURRENT = TRUE
    WHERE CUR.CATEGORY_NAME IS NULL;
""")

v.get('LOG').close()
