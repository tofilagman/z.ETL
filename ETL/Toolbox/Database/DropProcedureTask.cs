﻿using z.ETL.ConnectionManager;

namespace z.ETL.ControlFlow
{
    /// <summary>
    /// Drops a procedure. Use DropIfExists to drop a procedure only if it exists.
    /// </summary>
    public class DropProcedureTask : DropTask<IfProcedureExistsTask>, ITask
    {
        internal override string GetSql()
        {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");
            return $@"DROP PROCEDURE {ON.QuotatedFullName}";
        }

        public DropProcedureTask()
        {
        }

        public DropProcedureTask(string procedureName) : this()
        {
            ObjectName = procedureName;
        }

        public static void Drop(string procedureName)
            => new DropProcedureTask(procedureName).Drop();
        public static void Drop(IConnectionManager connectionManager, string procedureName)
            => new DropProcedureTask(procedureName) { ConnectionManager = connectionManager }.Drop();
        public static void DropIfExists(string procedureName)
            => new DropProcedureTask(procedureName).DropIfExists();
        public static void DropIfExists(IConnectionManager connectionManager, string procedureName)
            => new DropProcedureTask(procedureName) { ConnectionManager = connectionManager }.DropIfExists();



    }


}
