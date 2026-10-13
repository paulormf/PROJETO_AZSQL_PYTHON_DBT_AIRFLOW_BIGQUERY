/*
Enable table-level CDC for the project's CDC incremental subset.

Review and keep this list aligned with incremental_strategy: cdc in config/gcs_exports.yaml.
Run manually against the source database with an authorized database owner account.
This script is idempotent: already tracked tables are skipped.
*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE database_id = DB_ID()
      AND is_cdc_enabled = 1
)
BEGIN
    ;THROW 51000, 'CDC must be enabled at the database level first.', 1;
END;

DECLARE @targets TABLE (
    ordinal int IDENTITY(1, 1) PRIMARY KEY,
    schema_name sysname NOT NULL,
    table_name sysname NOT NULL
);

INSERT INTO @targets (schema_name, table_name)
VALUES
    (N'dbo', N'order_items'),
    (N'dbo', N'orders'),
    (N'dbo', N'products');


DECLARE @ordinal int = 1;
DECLARE @target_count int = (SELECT COUNT(*) FROM @targets);
DECLARE @schema_name sysname;
DECLARE @table_name sysname;

WHILE @ordinal <= @target_count
BEGIN
    SELECT
        @schema_name = schema_name,
        @table_name = table_name
    FROM @targets
    WHERE ordinal = @ordinal;

    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables AS t
        JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        WHERE s.name = @schema_name
          AND t.name = @table_name
    )
    BEGIN
        DECLARE @missing_table_message nvarchar(2048) =
            CONCAT(N'Source table does not exist: ', @schema_name, N'.', @table_name);
        ;THROW 51001, @missing_table_message, 1;
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables AS t
        JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        WHERE s.name = @schema_name
          AND t.name = @table_name
          AND t.is_tracked_by_cdc = 1
    )
    BEGIN
        EXEC sys.sp_cdc_enable_table
            @source_schema = @schema_name,
            @source_name = @table_name,
            @role_name = NULL,
            @supports_net_changes = 1;

        PRINT CONCAT(N'CDC enabled: ', @schema_name, N'.', @table_name);
    END
    ELSE
    BEGIN
        PRINT CONCAT(N'Already tracked, skipped: ', @schema_name, N'.', @table_name);
    END;

    SET @ordinal += 1;
END;
GO
