import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function
connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(connection_string).execution_options(autocommit=True)
engine.connect()

# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
engine.execute("""CREATE TABLE IF NOT EXISTS authors (
    author_id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL
);""")

# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function
engine.execute("INSERT INTO authors (first_name, last_name) VALUES ('John', 'Doe');")
engine.execute("INSERT INTO authors (first_name, last_name) VALUES ('Jane', 'Smith');")
engine.execute("INSERT INTO authors (first_name, last_name) VALUES ('Bob', 'Johnson');")

# 4) Use pandas to print the 'authors' table as a DataFrame using read_sql function
result_dataFrame = pd.read_sql("SELECT * FROM authors;", engine)
print(result_dataFrame)