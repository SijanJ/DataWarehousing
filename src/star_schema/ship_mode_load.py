
from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "SHIP_MODE_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_SHIP_MODE")
v.set("TMP_TABLE", "TMP_D_SHIP_MODE")
v.set("TGT_TABLE", "TGT_D_SHIP_MODE")
sf = Config(v)

# Truncate the temporary table
truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

# Load to temporary table
temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (SHIP_MODE)
                SELECT DISTINCT SHIP_MODE
                FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
            """
sf.execute_query(temp_query)

# SCD2 handling for ship mode dimension
# expire existing if value changed (not really applicable)
sf.execute_query(f"""
    UPDATE {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
    SET IS_CURRENT = FALSE,
        EFFECTIVE_TO = CURRENT_TIMESTAMP()
    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
    WHERE TGT.SHIP_MODE = TMP.SHIP_MODE
      AND TGT.IS_CURRENT = TRUE
      AND TGT.SHIP_MODE <> TMP.SHIP_MODE
""")

sf.execute_query(f"""
    INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} (SHIP_MODE, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
    SELECT TMP.SHIP_MODE, TRUE, CURRENT_TIMESTAMP(), TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')
    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
    LEFT JOIN {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} CUR
        ON CUR.SHIP_MODE = TMP.SHIP_MODE
       AND CUR.IS_CURRENT = TRUE
    WHERE CUR.SHIP_MODE IS NULL;
""")

v.get('LOG').close()