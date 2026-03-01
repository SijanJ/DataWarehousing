from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "CUSTOMER_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_CUSTOMER")
v.set("TMP_TABLE", "TMP_D_CUSTOMER")
v.set("TGT_TABLE", "TGT_D_CUSTOMER")
sf = Config(v)

# Truncate the temporary table
truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

# Load to temporary table
temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT)
                SELECT DISTINCT CUSTOMER_ID
                ,CUSTOMER_NAME
                ,SEGMENT                
                FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
            """
sf.execute_query(temp_query)

# UPDATE AND LOAD(Merge)
merge_query = f"""
                MERGE INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
                USING {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} AS TMP
                    ON TGT.CUSTOMER_ID = TMP.CUSTOMER_ID
                WHEN MATCHED THEN
                    UPDATE SET TGT.CUSTOMER_NAME = TMP.CUSTOMER_NAME, 
                    TGT.SEGMENT = TMP.SEGMENT
                WHEN NOT MATCHED THEN
                    INSERT (TGT.CUSTOMER_ID, TGT.CUSTOMER_NAME, TGT.SEGMENT)
                    VALUES (TMP.CUSTOMER_ID, TMP.CUSTOMER_NAME, TMP.SEGMENT);
            """
sf.execute_query(merge_query)

v.get('LOG').close()