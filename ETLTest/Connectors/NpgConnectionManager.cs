using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using z.ETL;
using z.ETL.ConnectionManager;

namespace ETLTest.Connectors
{
    public class NpgConnectionManager : DbConnectionManager<NpgsqlConnection>
    {
        TableDefinition DestTableDef { get; set; }
        Dictionary<string, TableColumn> DestinationColumns { get; set; }
 
        public NpgConnectionManager(string connectionString): 
            base(connectionString, ConnectionManagerType.Postgres)
        { }

        public override void AfterBulkInsert(string tableName)
        {
             
        }

        public override void BeforeBulkInsert(string tableName)
        {
            DestTableDef = TableDefinition.GetDefinitionFromTableName(this, tableName);
            DestinationColumns = new Dictionary<string, TableColumn>();
            foreach (var colDef in DestTableDef.Columns)
            {
                DestinationColumns.Add(colDef.Name, colDef);
            }
        }

        public override void BulkInsert(ITableData data, string tableName)
        {
            var TN = new ObjectNameDescriptor(tableName, ConnectionManagerType.Postgres);
            var sourceColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            var destColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();
            var quotedDestColumns = destColumnNames.Select(col => TN.QB + col + TN.QE);

            using (var writer = DbConnection.BeginBinaryImport($@"COPY {TN.QuotatedFullName} ({string.Join(", ", quotedDestColumns)}) FROM STDIN (FORMAT BINARY)"))
            {
                while (data.Read())
                {
                    writer.StartRow();
                    foreach (var destCol in destColumnNames)
                    {
                        TableColumn colDef = DestinationColumns[destCol];
                        int ordinal = data.GetOrdinal(destCol);
                        object val = data.GetValue(ordinal);
                        if (val != null)
                        {
                            object convertedVal = System.Convert.ChangeType(data.GetValue(ordinal), colDef.NETDataType);
                            writer.Write(convertedVal, colDef.InternalDataType.ToLower());
                        }
                        else
                        {
                            writer.WriteNull();
                        }
                    }
                }
                writer.Complete();
            }
        }

        public override IConnectionManager Clone()
        {
            if (LeaveOpen) return this;
            NpgConnectionManager clone = new NpgConnectionManager(ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
