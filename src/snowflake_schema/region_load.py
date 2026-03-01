from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "REGION_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_LOCATION")
v.set("TMP_TABLE", "TMP_D_REGION")
v.set("TGT_TABLE", "TGT_D_REGION")
sf = Config(v)

# truncate temporary staging table
sf.execute_query(f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}")

sf.execute_query(f"""
    INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} (REGION_NAME)
    SELECT DISTINCT REGION
    FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
    WHERE REGION IS NOT NULL
""" )

# merge into target dimension
sf.execute_query(f"""
    UPDATE {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
    SET
        IS_CURRENT = FALSE,
        EFFECTIVE_TO = CURRENT_TIMESTAMP()
    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
    WHERE TGT.REGION_NAME = TMP.REGION_NAME
      AND TGT.IS_CURRENT = TRUE
      AND TGT.REGION_NAME <> TMP.REGION_NAME
""")

sf.execute_query(f"""
    INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} (REGION_NAME, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
    SELECT TMP.REGION_NAME, TRUE, CURRENT_TIMESTAMP(), TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')
    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
    LEFT JOIN {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} CUR
        ON CUR.REGION_NAME = TMP.REGION_NAME
       AND CUR.IS_CURRENT = TRUE
    WHERE CUR.REGION_NAME IS NULL;
""" )

v.get('LOG').close()
