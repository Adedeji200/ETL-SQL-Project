# import necessary libraries
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg2
import os

# confgure PG connection
user=os.getenv('DB_USER')
password=os.getenv('DB_PASSWORD')
port=os.getenv('DB_PORT')
host=os.getenv('DB_HOST')
name=os.getenv('DB_NAME')

#create the sqlalchemy engine
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}")

# folder conatining csv data
csv_folder= "Data"

# loop through the csv file and load theem in PG
def load_csv_to_postgres():
    for file in os.listdir(csv_folder):
        if file.endswith(".csv"):
            table_name=file.split(".")[0]
            df = pd.read_csv(f"{csv_folder}/{file}", encoding = "ISO-8859-1")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Table {table_name} loaded successfully ")


if __name__ == "__main__":
    load_csv_to_postgres()
