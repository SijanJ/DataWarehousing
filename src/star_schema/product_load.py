from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "PRODUCT_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_PRODUCT")
v.set("TMP_TABLE", "TMP_D_PRODUCT")
v.set("TGT_TABLE", "TGT_D_PRODUCT")
sf = Config(v)

# Truncate the temporary table
truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

# Load to temporary table
temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUB_CATEGORY)
                SELECT DISTINCT PRODUCT_ID
                ,PRODUCT_NAME
                ,CATEGORY      
                ,SUB_CATEGORY          
                FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
            """
sf.execute_query(temp_query)

# UPDATE AND LOAD(Merge)
merge_query = f"""
                MERGE INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
                USING {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} AS TMP
                    ON TGT.PRODUCT_ID = TMP.PRODUCT_ID
                WHEN MATCHED THEN
                    UPDATE SET TGT.PRODUCT_NAME = TMP.PRODUCT_NAME, 
                    TGT.CATEGORY = TMP.CATEGORY,
                    TGT.SUB_CATEGORY = TMP.SUB_CATEGORY
                WHEN NOT MATCHED THEN
                    INSERT (TGT.PRODUCT_ID, TGT.PRODUCT_NAME, TGT.CATEGORY, TGT.SUB_CATEGORY)
                    VALUES (TMP.PRODUCT_ID, TMP.PRODUCT_NAME, TMP.CATEGORY, TMP.SUB_CATEGORY);
            """
sf.execute_query(merge_query)

v.get('LOG').close()