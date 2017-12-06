import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import json
import psycopg2


class DB_CLASS:

    engine = create_engine('postgresql://postgres:123@localhost:5432/reon')

    def savetable(self, df, table_name):

        try:
            pd.DataFrame.to_sql(df, table_name, self.engine, if_exists='append')

        except Exception as e:
            print (str(e))
            return False

        else:
            print ("Table: "+table_name+" "+"inserted to database : "+"reon")
            return True


