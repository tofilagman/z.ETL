﻿using z.ETL.ConnectionManager;

namespace z.ETL.ControlFlow
{
    /// <summary>
    /// Drops a database. Use DropIfExists to drop a database only if it exists. In MySql, this will drop a schema.
    /// </summary>
    /// <example>
    /// <code>
    /// DropDatabaseTask.Delete("DemoDB");
    /// </code>
    /// </example>
    public class DropDatabaseTask : DropTask<IfDatabaseExistsTask>, ITask
    {
        internal override string GetSql()
        {

            if (ConnectionType == ConnectionManagerType.SqlServer)
            {
                return
$@"
USE [master]
ALTER DATABASE [{ObjectName}]
SET SINGLE_USER WITH ROLLBACK IMMEDIATE
ALTER DATABASE [{ObjectName}]
SET MULTI_USER
DROP DATABASE [{ObjectName}]  
";
            }
            else if (ConnectionType == ConnectionManagerType.SQLite)
            {
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");
            }
            else
            {
                return $@"DROP DATABASE {ON.QuotatedObjectName}";
            }
        }

        public DropDatabaseTask()
        {
        }

        public DropDatabaseTask(string databaseName) : this()
        {
            ObjectName = databaseName;
        }

        public static void Drop(string databaseName)
            => new DropDatabaseTask(databaseName).Drop();
        public static void Drop(IConnectionManager connectionManager, string databaseName)
            => new DropDatabaseTask(databaseName) { ConnectionManager = connectionManager }.Drop();
        public static void DropIfExists(string databaseName)
            => new DropDatabaseTask(databaseName).DropIfExists();
        public static void DropIfExists(IConnectionManager connectionManager, string databaseName)
            => new DropDatabaseTask(databaseName) { ConnectionManager = connectionManager }.DropIfExists();
    }
}
