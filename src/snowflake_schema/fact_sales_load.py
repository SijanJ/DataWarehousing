from Config import Config
from Logger import Logger
from Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "FACT_SALES_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_F_SALES")
v.set("TMP_TABLE", "TMP_F_SALES_SF")
v.set("TGT_TABLE", "TGT_F_SALES_SF")
sf = Config(v)

# Truncate the temporary table
truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

# Load to temporary table
# TMP holds only raw natural keys and measures — NO surrogate keys
# All surrogate key lookups (CUSTOMER_KEY, PRODUCT_KEY, etc.) happen during the merge step
temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (ORDER_ID, CUSTOMER_ID, PRODUCT_ID, CITY_NAME, POSTAL_CODE, ORDER_DATE, SHIP_DATE, SHIP_MODE, QUANTITY, DISCOUNT,REVENUE, PROFIT, COST)
                SELECT
                    SRC.ORDER_ID
                    ,SRC.CUSTOMER_ID
                    ,SRC.PRODUCT_ID
                    ,SRC.CITY
                    ,SRC.POSTAL_CODE
                    ,SRC.ORDER_DATE
                    ,SRC.SHIP_DATE
                    ,SRC.SHIP_MODE
                    ,SUM(SRC.QUANTITY)
                    ,SUM(SRC.DISCOUNT)
                    ,SUM(SRC.REVENUE)
                    ,SUM(SRC.PROFIT)
                    ,SUM(SRC.COST)
                FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')} SRC
                GROUP BY
                    SRC.ORDER_ID
                    ,SRC.CUSTOMER_ID
                    ,SRC.PRODUCT_ID
                    ,SRC.CITY
                    ,SRC.POSTAL_CODE
                    ,SRC.ORDER_DATE
                    ,SRC.SHIP_DATE
                    ,SRC.SHIP_MODE
            """
sf.execute_query(temp_query)

# UPDATE AND LOAD (Merge)
# All surrogate key lookups are resolved here by joining TMP to each dimension TARGET table
merge_query = f"""
                MERGE INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
                USING (
                    SELECT
                        TMP.ORDER_ID
                        ,CUS.CUSTOMER_KEY
                        ,PRD.PRODUCT_KEY
                        ,CTY.CITY_KEY
                        ,ORD_DT.DATE_KEY    AS ORDER_DATE_KEY
                        ,SIP_DT.DATE_KEY    AS SHIP_DATE_KEY
                        ,SHIP.SHIP_MODE_KEY
                        ,TMP.QUANTITY
                        ,TMP.DISCOUNT
                        ,TMP.REVENUE
                        ,TMP.PROFIT
                        ,TMP.COST
                    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP

                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_CUSTOMER CUS
                        ON CUS.CUSTOMER_ID = TMP.CUSTOMER_ID

                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_PRODUCT PRD
                        ON PRD.PRODUCT_ID = TMP.PRODUCT_ID

                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_CITY CTY
                        ON CTY.CITY_NAME   = TMP.CITY_NAME
                        AND CTY.POSTAL_CODE = TMP.POSTAL_CODE

                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_DATE ORD_DT
                        ON ORD_DT.FULL_DATE = TMP.ORDER_DATE

                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_DATE SIP_DT
                        ON SIP_DT.FULL_DATE = TMP.SHIP_DATE

                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_SHIP_MODE SHIP
                        ON SHIP.SHIP_MODE = TMP.SHIP_MODE

                ) AS SRC
                    ON TGT.ORDER_ID = SRC.ORDER_ID
                WHEN MATCHED THEN
                    UPDATE SET
                        TGT.QUANTITY        = SRC.QUANTITY,
                        TGT.COST           = SRC.COST,
                        TGT.DISCOUNT        = SRC.DISCOUNT,
                        TGT.REVENUE         = SRC.REVENUE,
                        TGT.PROFIT          = SRC.PROFIT
                WHEN NOT MATCHED THEN
                    INSERT (
                        TGT.ORDER_ID,
                        TGT.CUSTOMER_KEY,
                        TGT.PRODUCT_KEY,
                        TGT.CITY_KEY,
                        TGT.ORDER_DATE_KEY,
                        TGT.SHIP_DATE_KEY,
                        TGT.SHIP_MODE_KEY,
                        TGT.QUANTITY,
                        TGT.DISCOUNT,
                        TGT.REVENUE,
                        TGT.PROFIT,
                        TGT.COST
                    )
                    VALUES (
                        SRC.ORDER_ID,
                        SRC.CUSTOMER_KEY,
                        SRC.PRODUCT_KEY,
                        SRC.CITY_KEY,
                        SRC.ORDER_DATE_KEY,
                        SRC.SHIP_DATE_KEY,
                        SRC.SHIP_MODE_KEY,
                        SRC.QUANTITY,
                        SRC.DISCOUNT,
                        SRC.REVENUE,
                        SRC.PROFIT,
                        SRC.COST
                    );
            """
sf.execute_query(merge_query)

v.get('LOG').close()