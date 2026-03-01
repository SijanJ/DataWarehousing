from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "LOCATION_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_LOCATION")
v.set("TMP_TABLE", "TMP_D_LOCATION")
v.set("TGT_TABLE", "TGT_D_LOCATION")
sf = Config(v)

# Truncate the temporary table
truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

# Load to temporary table
temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (COUNTRY, REGION, STATE, CITY, POSTAL_CODE)
                SELECT DISTINCT COUNTRY
                ,REGION
                ,STATE
                ,CITY
                ,POSTAL_CODE                
                FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
            """
sf.execute_query(temp_query)

# SCD2 handling for location dimension
src_cte = f"""
    SELECT COUNTRY, REGION, STATE, CITY, POSTAL_CODE
    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
"""

expire_query = f"""
    UPDATE {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
    SET
        IS_CURRENT = FALSE,
        EFFECTIVE_TO = CURRENT_TIMESTAMP()
    FROM (
        {src_cte}
    ) AS SRC
    WHERE TGT.POSTAL_CODE = SRC.POSTAL_CODE
      AND TGT.IS_CURRENT = TRUE
      AND (
            TGT.COUNTRY <> SRC.COUNTRY
         OR TGT.REGION  <> SRC.REGION
         OR TGT.STATE   <> SRC.STATE
         OR TGT.CITY    <> SRC.CITY
      );
"""
sf.execute_query(expire_query)

insert_query = f"""
    INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} (
        COUNTRY, REGION, STATE, CITY, POSTAL_CODE,
        IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO
    )
    SELECT
        SRC.COUNTRY, SRC.REGION, SRC.STATE, SRC.CITY, SRC.POSTAL_CODE,
        TRUE, CURRENT_TIMESTAMP(), TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')
    FROM (
        {src_cte}
    ) SRC
    LEFT JOIN {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} CUR
        ON CUR.POSTAL_CODE = SRC.POSTAL_CODE
       AND CUR.IS_CURRENT = TRUE
       AND CUR.COUNTRY = SRC.COUNTRY
       AND CUR.REGION  = SRC.REGION
       AND CUR.STATE   = SRC.STATE
       AND CUR.CITY    = SRC.CITY
    WHERE CUR.POSTAL_CODE IS NULL;
"""
sf.execute_query(insert_query)

v.get('LOG').close()