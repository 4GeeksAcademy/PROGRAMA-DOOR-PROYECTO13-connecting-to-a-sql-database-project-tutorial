import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# Cargar las variables del archivo .env
load_dotenv()


load
# Datos de conexión a la base de datos
host = 'localhost'
port = '5432'
username = 'gitpod'
password = 'postgres'

# Establecer conexión con PostgreSQL utilizando SQLAlchemy
connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(connection_string)


engine = create_engine(connection_string
# Nombre de la nueva base de datos que deseas crear
new_db_name = 
new
'nueva_base_de_datos'

# Comando SQL para crear la base de datos
create_db_query = 
create_db_query

create
f"CREATE DATABASE {new_db_name}"

# Conectarse a PostgreSQL
with engine.connect() as connection:
    
   
# Ejecutar el comando SQL para crear la base de datos
    connection.execute(create_db_query)


   
print(f"La base de datos '{new_db_name}' ha sido creada exitosamente.")

# Establecer conexión con la nueva base de datos
new_connection_string = 
new
f"postgresql://{username}:{password}@{host}:{port}/{new_db_name}"
engine = create_engine(new_connection_string)


engine = create_engine(new_connection
# Ejecutar las sentencias SQL para crear las tablas utilizando la función execute de SQLAlchemy
create_tables_query = 
create
"""
CREATE TABLE IF NOT EXISTS publishers (
    publisher_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (publisher_id)
);

CREATE TABLE IF NOT EXISTS authors (
    author_id INT NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    middle_name VARCHAR(50) NULL,
    last_name VARCHAR(100) NULL,
    PRIMARY KEY (author_id)
);

CREATE TABLE IF NOT EXISTS books (
    book_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    total_pages INT NULL,
    rating DECIMAL(4, 2) NULL,
    isbn VARCHAR(13) NULL,
    published_date DATE,
    publisher_id INT NULL,
    PRIMARY KEY (book_id),
    CONSTRAINT fk_publisher FOREIGN KEY (publisher_id) REFERENCES publishers (publisher_id)
);

CREATE TABLE IF NOT EXISTS book_authors (
    book_id INT NOT NULL,
    author_id INT NOT NULL,
    PRIMARY KEY (book_id, author_id),
    CONSTRAINT fk_book FOREIGN KEY (book_id) REFERENCES books (book_id) ON DELETE CASCADE,
    CONSTRAINT fk_author FOREIGN KEY (author_id) REFERENCES authors (author_id) ON DELETE CASCADE
);
"""

engine.execute(create_tables_query)



engine


# Ejecutar las sentencias SQL para insertar los datos utilizando la función execute de SQLAlchemy
insert_data_query = 
insert_data

insert
"INSERT INTO publishers (publisher_id, name) VALUES (33, 'quiensabe');"
engine.execute(insert_data_query)


engine
# Utilizar pandas para imprimir una de las tablas como DataFrame utilizando la función read_sql
table_name = "publishers"
df = pd.read_sql(
df =

df
f"SELECT * FROM {table_name}", engine)

# Imprimir el DataFrame
print(df)

``