#!/usr/bin/env python3
"""Entidades de origen, conexiones y lectura incremental de RDS MySQL y DocumentDB."""

import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

import pymysql
from bson import json_util
from pymongo import MongoClient
from pymysql.cursors import SSCursor

BATCH_SIZE = 1000
# Columnas de metadatos que el extractor agrega a cada fila del CSV.
METADATA_COLUMNS = {"_batch_id": "VARCHAR(64)", "_extracted_at": "TIMESTAMP", "_source": "VARCHAR(64)"}


@dataclass(frozen=True)
class Entity:
    """Una tabla o colección de origen y su tabla raw en Redshift.

    watermark: columna que define el avance incremental.
    watermark_kind: "timestamp" (se compara con >= para no perder filas con la misma marca)
    o "integer" (llave creciente, se compara con >).
    """

    source: str
    name: str
    columns: dict
    watermark: str
    watermark_kind: str

    @property
    def raw_table(self):
        prefix = "mysql" if self.source == "mysql" else "mongo"
        return f"raw_data.{prefix}_{self.name}"

    @property
    def source_label(self):
        return f"rds_mysql.{self.name}" if self.source == "mysql" else f"documentdb.{self.name}"

    @property
    def all_columns(self):
        return {**self.columns, **METADATA_COLUMNS}

    def parse_watermark(self, value):
        if value is None or value == "":
            return None
        return datetime.fromisoformat(value) if self.watermark_kind == "timestamp" else int(value)


MYSQL_ENTITIES = [
    Entity("mysql", "customers", {
        "customer_id": "BIGINT", "name": "VARCHAR(512)", "email": "VARCHAR(512)", "phone": "VARCHAR(64)",
        "city": "VARCHAR(256)", "department": "VARCHAR(256)", "registration_date": "DATE", "updated_at": "TIMESTAMP",
    }, "updated_at", "timestamp"),
    Entity("mysql", "products", {
        "product_id": "BIGINT", "name": "VARCHAR(512)", "category": "VARCHAR(256)", "price": "DECIMAL(12,2)",
        "description": "VARCHAR(2048)", "is_active": "BOOLEAN", "updated_at": "TIMESTAMP",
    }, "updated_at", "timestamp"),
    Entity("mysql", "orders", {
        "order_id": "BIGINT", "customer_id": "BIGINT", "order_date": "TIMESTAMP", "status": "VARCHAR(64)",
        "sales_channel": "VARCHAR(64)", "payment_method": "VARCHAR(64)", "total": "DECIMAL(14,2)",
    }, "order_id", "integer"),
    Entity("mysql", "order_items", {
        "order_item_id": "BIGINT", "order_id": "BIGINT", "product_id": "BIGINT", "quantity": "INTEGER",
        "unit_price": "DECIMAL(12,2)", "subtotal": "DECIMAL(14,2)",
    }, "order_item_id", "integer"),
]
MONGO_ENTITIES = [
    Entity("documentdb", "reviews", {
        "review_id": "BIGINT", "customer_id": "BIGINT", "product_id": "BIGINT", "rating": "INTEGER",
        "reviewed_at": "TIMESTAMP", "document_json": "VARCHAR(65535)",
    }, "review_id", "integer"),
    Entity("documentdb", "navigation_events", {
        "event_id": "BIGINT", "event_type": "VARCHAR(128)", "occurred_at": "TIMESTAMP", "customer_id": "BIGINT",
        "product_id": "BIGINT", "sales_channel": "VARCHAR(64)", "document_json": "VARCHAR(65535)",
    }, "event_id", "integer"),
]
ENTITIES = MYSQL_ENTITIES + MONGO_ENTITIES


def connect_mysql():
    return pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.getenv("MYSQL_DATABASE", "ecommerce"),
        charset="utf8mb4",
        cursorclass=SSCursor,
        connect_timeout=15,
    )


def connect_mongo():
    """DocumentDB: TLS con el bundle de AWS y retryWrites=false vienen en MONGODB_URI."""
    return MongoClient(os.environ["MONGODB_URI"], tz_aware=True, serverSelectionTimeoutMS=15000)


def to_utc_naive(value):
    """DocumentDB devuelve fechas UTC; Redshift las guarda como TIMESTAMP en UTC."""
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def csv_value(value):
    if isinstance(value, datetime):
        return to_utc_naive(value).isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def mysql_rows(entity, watermark_from):
    """Lee la tabla completa o solo lo posterior a la marca de agua. Devuelve un generador de tuplas."""
    columns = ", ".join(entity.columns)
    query = f"SELECT {columns} FROM {entity.name}"
    params = ()
    if watermark_from is not None:
        operator = ">=" if entity.watermark_kind == "timestamp" else ">"
        query += f" WHERE {entity.watermark} {operator} %s"
        params = (watermark_from,)
    query += f" ORDER BY {entity.watermark}, {next(iter(entity.columns))}"

    connection = connect_mysql()
    try:
        cursor = connection.cursor()
        cursor.execute(query, params)
        while batch := cursor.fetchmany(BATCH_SIZE):
            yield from batch
        cursor.close()
    finally:
        connection.close()


def flatten_mongo(collection, document):
    """Aplana un documento a las columnas raw; conserva el documento completo en document_json."""
    customer = document.get("customer") or {}
    product = document.get("product") or {}
    if collection == "reviews":
        return (
            document.get("review_id"), customer.get("customer_id"), product.get("product_id"),
            document.get("rating"), document.get("reviewed_at"), json_util.dumps(document),
        )
    device = document.get("device") or {}
    return (
        document.get("event_id"), document.get("event_type"), document.get("occurred_at"),
        customer.get("customer_id"), product.get("product_id"), device.get("sales_channel"),
        json_util.dumps(document),
    )


def mongo_rows(entity, watermark_from):
    query = {} if watermark_from is None else {entity.watermark: {"$gt": watermark_from}}
    client = connect_mongo()
    try:
        database = client[os.getenv("MONGO_DATABASE", "ecommerce")]
        cursor = database[entity.name].find(query).sort(entity.watermark, 1).batch_size(BATCH_SIZE)
        for document in cursor:
            yield flatten_mongo(entity.name, document)
    finally:
        client.close()


def source_rows(entity, watermark_from):
    reader = mysql_rows if entity.source == "mysql" else mongo_rows
    return reader(entity, watermark_from)
