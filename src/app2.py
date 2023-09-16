import os
from sqlalchemy import create_engine, text  # Importa 'text' de SQLAlchemy
import pandas as pd
from dotenv import load_dotenv

# Cargar las variables del archivo .env
load_dotenv()

# Datos de conexión a la base de datos
host = 'localhost'
port = '5432'
username = 'gitpod'
password = 'postgres'

# Establecer conexión con PostgreSQL utilizando SQLAlchemy
connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

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

# Ejecutar las consultas SQL utilizando 'text' de SQLAlchemy
with engine.connect() as connection:
    connection.execute(text(create_tables_query))

# 3) Ejecutar las sentencias SQL para insertar los datos utilizando la función execute de SQLAlchemy
insert_data_query = "INSERT INTO publishers(publisher_id, name) VALUES (33, 'quiensabe');"
with engine.connect() as connection:
    connection.execute(text(insert_data_query))

# Consulta para insertar datos en la tabla "authors"
insert_authors_query = """
INSERT INTO authors (author_id, first_name, middle_name, last_name)
VALUES
    (1, 'Nombre1', 'Apellido1', 'Apellido2'),
    (2, 'Nombre2', NULL, 'Apellido3');
"""

with engine.connect() as connection:
    connection.execute(text(insert_authors_query))

# Consulta para insertar datos en la tabla "books"
insert_books_query = """
INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id)
VALUES
    (101, 'Libro1', 300, 4.5, '1234567890', '2022-01-01', 33),
    (102, 'Libro2', 250, 4.0, '0987654321', '2021-12-15', 33);
"""

with engine.connect() as connection:
    connection.execute(text(insert_books_query))

# Consulta para insertar datos en la tabla "book_authors"
insert_book_authors_query = """
INSERT INTO book_authors (book_id, author_id)
VALUES
    (101, 1),
    (102, 2);
"""

with engine.connect() as connection:
    connection.execute(text(insert_book_authors_query))

# 4) Utilizar pandas para imprimir una de las tablas como DataFrame utilizando la función read_sql
table_name = "publishers"
df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

# Imprimir el DataFrame
print(df)
