-- =====================================================================
-- Project : Identifying & Retaining High-Value Customers (Online Retail)
-- File    : 01_create_schema.sql   (run in DBeaver -> SQL Editor, Alt+X)
-- Database: retail_clv   (create it first:  CREATE DATABASE retail_clv; )
-- =====================================================================
DROP SCHEMA IF EXISTS retail CASCADE;
CREATE SCHEMA retail;
SET search_path TO retail;

-- 1) Staging table: raw file loaded exactly as received (no constraints)
CREATE TABLE stg_online_retail (
    invoice_no   TEXT,
    stock_code   TEXT,
    description  TEXT,
    quantity     INTEGER,
    invoice_date TIMESTAMP,
    unit_price   NUMERIC(12,3),
    customer_id  NUMERIC(10,1),
    country      TEXT
);

-- 2) Dimension tables
CREATE TABLE dim_customer (
    customer_id  INTEGER PRIMARY KEY,
    country      TEXT NOT NULL
);

CREATE TABLE dim_product (
    stock_code   TEXT PRIMARY KEY,
    description  TEXT
);

-- 3) Fact tables (one row per invoice, one row per invoice line)
CREATE TABLE fact_invoice (
    invoice_no    TEXT PRIMARY KEY,
    customer_id   INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    invoice_date  TIMESTAMP NOT NULL
);

CREATE TABLE fact_invoice_line (
    line_id      BIGSERIAL PRIMARY KEY,
    invoice_no   TEXT    NOT NULL REFERENCES fact_invoice(invoice_no),
    stock_code   TEXT    NOT NULL REFERENCES dim_product(stock_code),
    quantity     INTEGER NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(12,3) NOT NULL CHECK (unit_price > 0)
);

-- Returns (invoices starting with 'C') are kept separately
CREATE TABLE fact_return_line (
    line_id      BIGSERIAL PRIMARY KEY,
    invoice_no   TEXT NOT NULL,
    customer_id  INTEGER NOT NULL,
    stock_code   TEXT NOT NULL,
    quantity     INTEGER NOT NULL,
    unit_price   NUMERIC(12,3) NOT NULL,
    invoice_date TIMESTAMP NOT NULL
);

CREATE INDEX ix_invoice_customer ON fact_invoice(customer_id);
CREATE INDEX ix_invoice_date     ON fact_invoice(invoice_date);
CREATE INDEX ix_line_invoice     ON fact_invoice_line(invoice_no);
CREATE INDEX ix_line_stock       ON fact_invoice_line(stock_code);
