# Databricks notebook source
# MAGIC %sql
# MAGIC -- Reset all user schemas in brickwell_health_dev catalog
# MAGIC -- WARNING: This will permanently delete all data in these schemas!
# MAGIC
# MAGIC -- CREATE Catalog
# MAGIC CREATE CATALOG IF NOT EXISTS brickwell_health_dev;

# MAGIC -- Drop all schemas CASCADE
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev._meta CASCADE;
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_gold_claims CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_gold_billing CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_gold_policy CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_gold_reference CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_gold_regulatory CASCADE;
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_silver_claims CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_silver_billing CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_silver_policy CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_silver_reference CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_silver_regulatory CASCADE;
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_bronze_claims CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_bronze_billing CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_bronze_policy CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_bronze_reference CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_bronze_regulatory CASCADE;
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_raw_claims CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_raw_policy CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_raw_reference CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_raw_regulatory CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_raw_billing CASCADE;
# MAGIC
# MAGIC -- Recreate all schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev._meta;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_gold_claims;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_gold_billing;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_gold_policy;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_gold_reference;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_gold_regulatory;
# MAGIC
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_silver_claims;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_silver_billing;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_silver_policy;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_silver_reference;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_silver_regulatory;
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_bronze_claims;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_bronze_billing;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_bronze_policy;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_bronze_reference;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_bronze_regulatory;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_raw_claims;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_raw_policy;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_raw_reference;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_raw_regulatory;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_raw_billing;
# MAGIC
# MAGIC
# MAGIC -- Recreate all volumes
# MAGIC CREATE VOLUME brickwell_health_dev.edw_raw_claims.landing;
# MAGIC CREATE VOLUME brickwell_health_dev.edw_raw_policy.landing;
# MAGIC CREATE VOLUME brickwell_health_dev.edw_raw_reference.landing;
# MAGIC CREATE VOLUME brickwell_health_dev.edw_raw_regulatory.landing;
# MAGIC CREATE VOLUME brickwell_health_dev.edw_raw_billing.landing;
# MAGIC
# MAGIC CREATE VOLUME brickwell_health_dev.edw_raw_claims.demo_valut;
# MAGIC CREATE VOLUME brickwell_health_dev.edw_raw_policy.demo_valut;
# MAGIC CREATE VOLUME brickwell_health_dev.edw_raw_reference.demo_valut;
# MAGIC CREATE VOLUME brickwell_health_dev.edw_raw_regulatory.demo_valut;
# MAGIC CREATE VOLUME brickwell_health_dev.edw_raw_billing.demo_valut;