from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "PRODUCT_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_PRODUCT")
v.set("TMP_TABLE", "TMP_D_PRODUCT_SF")
v.set("TGT_TABLE", "TGT_D_PRODUCT_SF")
sf = Config(v)

# Truncate temporary table
truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

# Load to temporary table
temp_query = f"""
    INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
    (PRODUCT_ID, PRODUCT_NAME, SUBCATEGORY_NAME)
    SELECT DISTINCT
        PRODUCT_ID,
        PRODUCT_NAME,
        SUB_CATEGORY
    FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
"""
sf.execute_query(temp_query)

# SCD2 handling: expire changed current records then insert
src_cte = f"""
    SELECT
        TMP.PRODUCT_ID,
        TMP.PRODUCT_NAME,
        SC.SUBCATEGORY_KEY
    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
    JOIN {v.get('TGT_SCHEMA')}.TGT_D_SUBCATEGORY SC
        ON TMP.SUBCATEGORY_NAME = SC.SUBCATEGORY_NAME
"""

# expire where attributes differ
expire_query = f"""
    UPDATE {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
    SET
        IS_CURRENT = FALSE,
        EFFECTIVE_TO = CURRENT_TIMESTAMP()
    FROM (
        {src_cte}
    ) AS SRC
    WHERE TGT.PRODUCT_ID = SRC.PRODUCT_ID
      AND TGT.IS_CURRENT = TRUE
      AND (
            TGT.PRODUCT_NAME    <> SRC.PRODUCT_NAME
         OR TGT.SUBCATEGORY_KEY <> SRC.SUBCATEGORY_KEY
      );
"""
sf.execute_query(expire_query)

# insert new/current records where no active row exists
insert_query = f"""
    INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} (
        PRODUCT_ID,
        PRODUCT_NAME,
        SUBCATEGORY_KEY,
        IS_CURRENT,
        EFFECTIVE_FROM,
        EFFECTIVE_TO
    )
    SELECT
        SRC.PRODUCT_ID,
        SRC.PRODUCT_NAME,
        SRC.SUBCATEGORY_KEY,
        TRUE,
        CURRENT_TIMESTAMP(),
        TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')
    FROM (
        {src_cte}
    ) SRC
    LEFT JOIN {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} CUR
        ON CUR.PRODUCT_ID = SRC.PRODUCT_ID
       AND CUR.IS_CURRENT = TRUE
       AND CUR.PRODUCT_NAME = SRC.PRODUCT_NAME
       AND CUR.SUBCATEGORY_KEY = SRC.SUBCATEGORY_KEY
    WHERE CUR.PRODUCT_ID IS NULL;
"""
sf.execute_query(insert_query)

v.get('LOG').close()