-- Active: 1750346197428@@localhost@1433@master
USE master;
GO

CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

-- Créer les schémas si inexistants (T-SQL ne supporte pas "IF NOT EXISTS" pour les schémas, donc vous pouvez ignorer l'erreur si besoin ou ajouter une vérification dynamique)
IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name = 'BRONZE_LAYER'
)
    EXEC('CREATE SCHEMA BRONZE_LAYER');
GO

IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name = 'SILVER_LAYER'
)
    EXEC('CREATE SCHEMA SILVER_LAYER');
GO

IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name = 'GOLD_LAYER'
)
    EXEC('CREATE SCHEMA GOLD_LAYER');
GO
