using z.ETL.ConnectionManager;
using z.ETL.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;
using Microsoft.Extensions.Logging;

namespace z.ETL.DataFlow
{
    /// <summary>
    /// A database destination defines a table where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// </summary>
    /// <see cref="DbDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    /// <example>
    /// <code>
    /// DbDestination&lt;MyRow&gt; dest = new DbDestination&lt;MyRow&gt;("dbo.table");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class DbDestination<TInput> : DataFlowBatchDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => $"Write data into table {DestinationTableDefinition?.Name ?? TableName}";
        /* Public properties */
        public TableDefinition DestinationTableDefinition { get; set; }
        public bool HasDestinationTableDefinition => DestinationTableDefinition != null;
        public string TableName { get; set; }
        public bool HasTableName => !String.IsNullOrWhiteSpace(TableName);
        internal TypeInfo TypeInfo { get; set; }


        internal const int DEFAULT_BATCH_SIZE = 1000;


        public DbDestination(ILogger logger)
        {
            BatchSize = DEFAULT_BATCH_SIZE;
            Logger = logger;
        }

        public DbDestination(int batchSize, ILogger logger)
        {
            BatchSize = batchSize;
            Logger = logger;
        }

        public DbDestination(string tableName, ILogger logger) : this(logger)
        {
            TableName = tableName;
        }

        public DbDestination(IConnectionManager connectionManager, string tableName, ILogger logger) : this(tableName, logger)
        {
            ConnectionManager = connectionManager;
        }

        public DbDestination(string tableName, int batchSize, ILogger logger) : this(batchSize, logger)
        {
            TableName = tableName;
        }

        public DbDestination(IConnectionManager connectionManager, string tableName, int batchSize, ILogger logger) : this(tableName, batchSize, logger)
        {
            ConnectionManager = connectionManager;
        }

        protected override void InitObjects(int batchSize)
        {
            base.InitObjects(batchSize);
            TypeInfo = new TypeInfo(typeof(TInput));
        }

        protected override void WriteBatch(ref TInput[] data)
        {
            if (!HasDestinationTableDefinition) LoadTableDefinitionFromTableName();

            base.WriteBatch(ref data);

            TryBulkInsertData(data);

            LogProgressBatch(data.Length);
        }

        private void LoadTableDefinitionFromTableName()
        {
            if (HasTableName)
                DestinationTableDefinition = TableDefinition.GetDefinitionFromTableName(this.DbConnectionManager, TableName);
            else if (!HasDestinationTableDefinition && !HasTableName)
                throw new ETLBoxException("No Table definition or table name found! You must provide a table name or a table definition.");
        }

        private void TryBulkInsertData(TInput[] data)
        {
            TableData<TInput> td = CreateTableDataObject(ref data);
            try
            {
                new SqlTask(this, $"Execute Bulk insert")
                {
                    DisableLogging = true
                }
                .BulkInsert(td, DestinationTableDefinition.Name);
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer) throw e;
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TInput[]>(data));
            }
        }

        private TableData<TInput> CreateTableDataObject(ref TInput[] data)
        {
            TableData<TInput> td = new TableData<TInput>(DestinationTableDefinition, DEFAULT_BATCH_SIZE);
            td.Rows = ConvertRows(ref data);
            if (TypeInfo.IsDynamic && data.Length > 0)
                foreach (var column in (IDictionary<string, object>)data[0])
                    td.DynamicColumnNames.Add(column.Key);
            return td;
        }

        private List<object[]> ConvertRows(ref TInput[] data)
        {
            List<object[]> result = new List<object[]>();
            foreach (var CurrentRow in data)
            {
                if (CurrentRow == null) continue;
                object[] rowResult;
                if (TypeInfo.IsArray)
                {
                    rowResult = CurrentRow as object[];
                }
                else if (TypeInfo.IsDynamic)
                {
                    IDictionary<string, object> propertyValues = (IDictionary<string, object>)CurrentRow;
                    rowResult = new object[propertyValues.Count];
                    int index = 0;
                    foreach (var prop in propertyValues)
                    {
                        rowResult[index] = prop.Value;
                        index++;
                    }
                }
                else
                {
                    rowResult = new object[TypeInfo.PropertyLength];
                    int index = 0;
                    foreach (PropertyInfo propInfo in TypeInfo.Properties)
                    {
                        rowResult[index] = propInfo.GetValue(CurrentRow);
                        index++;
                    }
                }
                result.Add(rowResult);
            }
            return result;
        }
    }

    /// <summary>
    /// A database destination defines a table where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// The DbDestination uses a dynamic object as input type. If you need other data types, use the generic DbDestination instead.
    /// </summary>
    /// <see cref="DbDestination{TInput}"/>
    /// <example>
    /// <code>
    /// //Non generic DbDestination works with dynamic object as input
    /// //use DbDestination&lt;TInput&gt; for generic usage!
    /// DbDestination dest = new DbDestination("dbo.table");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class DbDestination : DbDestination<ExpandoObject>
    {
        public DbDestination(ILogger logger) : base(logger) { }

        public DbDestination(int batchSize, ILogger logger) : base(batchSize, logger) { }

        public DbDestination(string tableName, ILogger logger) : base(tableName, logger) { }

        public DbDestination(IConnectionManager connectionManager, string tableName, ILogger logger) : base(connectionManager, tableName, logger) { }

        public DbDestination(string tableName, int batchSize, ILogger logger) : base(tableName, batchSize, logger) { }

        public DbDestination(IConnectionManager connectionManager, string tableName, int batchSize, ILogger logger) : base(connectionManager, tableName, batchSize, logger) { }
    }

}
