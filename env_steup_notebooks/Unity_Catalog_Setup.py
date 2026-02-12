# Databricks notebook source
# MAGIC %sql
# MAGIC -- Reset all user schemas in brickwell_health_dev catalog
# MAGIC -- WARNING: This will permanently delete all data in these schemas!
# MAGIC 
# MAGIC -- CREATE Catalog
# MAGIC CREATE CATALOG IF NOT EXISTS brickwell_health_dev;
# MAGIC 
# MAGIC -- Drop all schemas CASCADE
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev._meta CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev._vault CASCADE;
# MAGIC DROP SCHEMA IF EXISTS brickwell_health_dev.edw_staging CASCADE;
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
# MAGIC 
# MAGIC -- Recreate all schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev._meta;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev._vault;
# MAGIC CREATE SCHEMA IF NOT EXISTS brickwell_health_dev.edw_staging;
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
# MAGIC 
# MAGIC -- Recreate all volumes
# MAGIC 
# MAGIC CREATE VOLUME brickwell_health_dev._vault.store;
# MAGIC CREATE VOLUME brickwell_health_dev.edw_staging.incoming;