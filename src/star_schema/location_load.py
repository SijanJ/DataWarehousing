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

# UPDATE AND LOAD(Merge)
merge_query = f"""
                MERGE INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
                USING {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} AS TMP
                    ON TGT.POSTAL_CODE = TMP.POSTAL_CODE
                WHEN MATCHED THEN
                    UPDATE SET TGT.COUNTRY = TMP.COUNTRY, 
                    TGT.REGION = TMP.REGION,
                    TGT.STATE = TMP.STATE,
                    TGT.CITY = TMP.CITY
                WHEN NOT MATCHED THEN
                    INSERT (TGT.COUNTRY, TGT.REGION, TGT.STATE, TGT.CITY, TGT.POSTAL_CODE)
                    VALUES (TMP.COUNTRY, TMP.REGION, TMP.STATE, TMP.CITY, TMP.POSTAL_CODE);
            """
sf.execute_query(merge_query)

v.get('LOG').close()