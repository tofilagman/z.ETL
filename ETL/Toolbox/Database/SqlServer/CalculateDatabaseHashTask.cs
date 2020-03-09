﻿using z.ETL.ConnectionManager;
using z.ETL.Helper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace z.ETL.ControlFlow.SqlServer
{
    /// <summary>
    /// Calculates a hash value of the database. It will use only the schemas given in the property SchemaName for the calculation.
    /// The hash calcualtion is based only on the user tables in the schema.
    /// </summary>
    /// <example>
    /// <code>
    /// CalculateDatabaseHashTask.Calculate(new List&lt;string&gt;() { "demo", "dbo" });
    /// </code>
    /// </example>
    public class CalculateDatabaseHashTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Calculate hash value for schema(s) {SchemaNamesAsString}";
        public void Execute()
        {
            if (ConnectionType != ConnectionManagerType.SqlServer)
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");
            List<string> allColumns = new List<string>();
            new SqlTask(this, Sql)
            {
                Actions = new List<Action<object>>() {
                    col => allColumns.Add((string)col)
                }
            }
                .ExecuteReader();
            DatabaseHash = HashHelper.Encrypt_Char40(String.Join("|", allColumns));
        }

        /* Public properties */
        public List<string> SchemaNames { get; set; }

        public string DatabaseHash { get; private set; }

        string SchemaNamesAsString => String.Join(",", SchemaNames.Select(name => $"'{name}'"));
        public string Sql => $@"
SELECT sch.name + '.' + tbls.name + N'|' + 
	   cols.name + N'|' + 
	   typ.name + N'|' + 
	   CAST(cols.max_length AS nvarchar(20))+ N'|' + 
	   CAST(cols.precision AS nvarchar(20)) + N'|' + 
	   CAST(cols.scale AS nvarchar(20)) + N'|' + 
	   CAST(cols.is_nullable AS nvarchar(3)) + N'|' + 
	   CAST(cols.is_identity AS nvarchar(3))+ N'|' + 
	   CAST(cols.is_computed AS nvarchar(3)) AS FullColumnName
FROM sys.columns cols
INNER join sys.tables tbls ON cols.object_id = tbls.object_id
INNER join sys.schemas sch ON sch.schema_id = tbls.schema_id
INNER join sys.types typ ON typ.user_type_id = cols.user_type_id
WHERE tbls.type = 'U'
AND sch.name IN ({SchemaNamesAsString})
ORDER BY sch.name, tbls.name, cols.column_id
";

        public CalculateDatabaseHashTask()
        {

        }
        public CalculateDatabaseHashTask(List<string> schemaNames) : this()
        {
            this.SchemaNames = schemaNames;
        }
        public CalculateDatabaseHashTask Calculate()
        {
            Execute();
            return this;
        }

        public static string Calculate(List<string> schemaNames) => new CalculateDatabaseHashTask(schemaNames).Calculate().DatabaseHash;
        public static string Calculate(IConnectionManager connectionManager, List<string> schemaNames)
            => new CalculateDatabaseHashTask(schemaNames) { ConnectionManager = connectionManager }.Calculate().DatabaseHash;


    }
}
