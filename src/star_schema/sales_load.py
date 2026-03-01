from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "SALES_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_F_SALES")
v.set("TMP_TABLE", "TMP_F_SALES")
v.set("TGT_TABLE", "TGT_F_SALES")
sf = Config(v)

# Truncate the temporary table
truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

# Load to temporary table
temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (ORDER_ID, CUSTOMER_KEY, PRODUCT_KEY, LOCATION_KEY, ORDER_DATE_KEY, SHIP_DATE_KEY, SHIP_MODE_KEY, QUANTITY, DISCOUNT, REVENUE, PROFIT, COST)
                SELECT
                    SRC.ORDER_ID 
                    ,CUS.CUSTOMER_KEY
                    ,PRD.PRODUCT_KEY
                    ,LOC.LOCATION_KEY
                    ,ORD_DT.DATE_KEY
                    ,SIP_DT.DATE_KEY
                    ,SHIP.SHIP_MODE_KEY
                    ,SUM(QUANTITY)
                    ,SUM(DISCOUNT)
                    ,SUM(REVENUE)
                    ,SUM(PROFIT)
                    ,SUM(COST)       
                FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')} SRC
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_CUSTOMER CUS
                    ON CUS.CUSTOMER_ID = SRC.CUSTOMER_ID
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_PRODUCT PRD
                    ON PRD.PRODUCT_ID = SRC.PRODUCT_ID
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_LOCATION LOC
                    ON LOC.COUNTRY = SRC.COUNTRY
                    AND LOC.REGION = SRC.REGION
                    AND LOC.STATE = SRC.STATE
                    AND LOC.CITY = SRC.CITY
                    AND LOC.POSTAL_CODE = SRC.POSTAL_CODE
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_DATE ORD_DT
                    ON ORD_DT.FULL_DATE = SRC.ORDER_DATE
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_DATE SIP_DT
                    ON SIP_DT.FULL_DATE = SRC.SHIP_DATE
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_SHIP_MODE SHIP
                    ON SHIP.SHIP_MODE = SRC.SHIP_MODE
                GROUP BY SRC.ORDER_ID, CUS.CUSTOMER_KEY, PRODUCT_KEY, LOCATION_KEY, ORD_DT.DATE_KEY, SIP_DT.DATE_KEY, SHIP.SHIP_MODE_KEY
            """
sf.execute_query(temp_query)

# UPDATE AND LOAD(Merge)
merge_query = f"""
                MERGE INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
                USING {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} AS TMP
                    ON TGT.ORDER_ID = TMP.ORDER_ID
                WHEN MATCHED THEN
                    UPDATE SET TGT.QUANTITY = TMP.QUANTITY, 
                    TGT.DISCOUNT = TMP.DISCOUNT,
                    TGT.REVENUE = TMP.REVENUE,
                    TGT.PROFIT = TMP.PROFIT,
                    TGT.COST = TMP.COST
                WHEN NOT MATCHED THEN
                    INSERT (TGT.ORDER_ID, TGT.CUSTOMER_KEY, TGT.PRODUCT_KEY, TGT.LOCATION_KEY, TGT.ORDER_DATE_KEY, TGT.SHIP_DATE_KEY, TGT.SHIP_MODE_KEY, TGT.QUANTITY, TGT.DISCOUNT, TGT.REVENUE, TGT.PROFIT, TGT.COST)
                    VALUES (TMP.ORDER_ID, TMP.CUSTOMER_KEY, TMP.PRODUCT_KEY, TMP.LOCATION_KEY, TMP.ORDER_DATE_KEY, TMP.SHIP_DATE_KEY, TMP.SHIP_MODE_KEY, TMP.QUANTITY, TMP.DISCOUNT, TMP.REVENUE, TMP.PROFIT, TMP.COST);
            """
sf.execute_query(merge_query)

v.get('LOG').close()