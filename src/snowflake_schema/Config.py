from Variable import Variables
from Logger import Logger
import snowflake.connector
           # pass Variables instance into Config
class Config:

    def __init__(self, v: Variables):
        self.v = v
        self.log = v.get("LOG")
        self.v.get("USER")
        self.USER = self.v.get("USER")
        self.PASSWORD = self.v.get("PASSWORD")
        self.ACCOUNT = self.v.get("ACCOUNT")
        self.DATABASE = self.v.get("DATABASE")
        self.DATA_WAREHOUSE = self.v.get("WAREHOUSE")
        ctx = snowflake.connector.connect(
            user=self.USER,
            password=self.PASSWORD,
            account=self.ACCOUNT,
            database=self.DATABASE,
            warehouse=self.DATA_WAREHOUSE,
            client_telemetry_enabled=False
        )
        self.cs = ctx.cursor()


    def execute_query(self, query):
        try:
            self.log.message(f"Executing query: {query}")
            self.cs.execute(query)
            val = self.cs.fetchall()
            self.log.message(f"Query Result: {val}")
            return val
        except Exception as e:
            self.log.error(f"query error: {query}")
            self.log.error(f"Error: {e}")

    def executemany(self, query, params):
        try:
            self.log.message(f"Executing query: {query} . Params: {params} ")
            self.cs.executemany(query, params)
            val = self.cs.fetchall()
            self.log.message(f"Query Result: {val}")
            return val
        except Exception as e:
            self.log.error(f"query error: {query}")
            self.log.error(f"Error: {e}")

