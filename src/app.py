
#createdb -h localhost -U gitpod nueva_base_de_datos
#psql -h localhost -U gitpod nueva_base_de_datos


import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# Cargar las variables del archivo .env
load_dotenv()

# Datos de conexión a la base de datos........
host = 'localhost'
port = '5432'
username = 'gitpod'
password = 'postgres'

# Establecer conexión con PostgreSQL utilizando SQLAlchemy
connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"

engine = create_engine(connection_string)


# 2) Ejecutar las sentencias SQL para crear las tablas utilizando la función execute de SQLAlchemy


create_tables_query = """
CREATE TABLE IF NOT EXISTS publishers(
    publisher_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY(publisher_id)
);

CREATE TABLE IF NOT EXISTS authors(
    author_id INT NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    middle_name VARCHAR(50) NULL,
    last_name VARCHAR(100) NULL,
    PRIMARY KEY(author_id)
);

CREATE TABLE IF NOT EXISTS books(
    book_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    total_pages INT NULL,
    rating DECIMAL(4, 2) NULL,
    isbn VARCHAR(13) NULL,
    published_date DATE,
    publisher_id INT NULL,
    PRIMARY KEY(book_id),
    CONSTRAINT fk_publisher FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id)
);

CREATE TABLE IF NOT EXISTS book_authors (
    book_id INT NOT NULL,
    author_id INT NOT NULL,
    PRIMARY KEY(book_id, author_id),
    CONSTRAINT fk_book FOREIGN KEY(book_id) REFERENCES books(book_id) ON DELETE CASCADE,
    CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(author_id) ON DELETE CASCADE
);
"""

engine.execute(create_tables_query)

# 3) Ejecutar las sentencias SQL para insertar los datos utilizando la función execute de SQLAlchemy
insert_data_query = "INSERT INTO publishers(publisher_id, name) VALUES (33, 'quiensabe');"
engine.execute(insert_data_query)

# 4) Utilizar pandas para imprimir una de las tablas como DataFrame utilizando la función read_sql
table_name = "publishers"
df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

# Imprimir el DataFrame
print(df)
#createdb -h localhost -U gitpod otronombre
#psql -h localhost -U gitpod otronombre