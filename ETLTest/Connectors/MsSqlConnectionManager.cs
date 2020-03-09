using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using z.ETL;
using z.ETL.ConnectionManager;

namespace ETLTest.Connectors
{
    public class MsSqlConnectionManager : DbConnectionManager<SqlConnection>
    {

        public bool ModifyDBSettings { get; set; } = false;
        private string PageVerify { get; set; }
        private string RecoveryModel { get; set; }

        public MsSqlConnectionManager(string connectionString) :
            base(connectionString, ConnectionManagerType.SqlServer)
        {

        }

        public override void AfterBulkInsert(string tableName)
        {
            if (ModifyDBSettings)
            {
                try
                {
                    string dbName = this.DbConnection.Database;
                    this.ExecuteNonQuery($@"USE master");
                    this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET PAGE_VERIFY {PageVerify};");
                    this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET RECOVERY {RecoveryModel}");
                    this.ExecuteNonQuery($@"USE [{dbName}]");
                }
                catch { }
            }
        }

        public override void BeforeBulkInsert(string tableName)
        {
            if (ModifyDBSettings)
            {
                try
                {
                    string dbName = this.DbConnection.Database;
                    PageVerify = this.ExecuteScalar($"SELECT page_verify_option_desc FROM sys.databases WHERE NAME = '{dbName}'").ToString();
                    RecoveryModel = this.ExecuteScalar($"SELECT recovery_model_desc FROM sys.databases WHERE NAME = '{dbName}'").ToString();
                    this.ExecuteNonQuery($@"USE master");
                    this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET PAGE_VERIFY NONE;");
                    this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET RECOVERY BULK_LOGGED");
                    this.ExecuteNonQuery($@"USE [{dbName}]");
                }
                catch
                {
                    ModifyDBSettings = false;
                }
            }
        }

        public override void BulkInsert(ITableData data, string tableName)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(DbConnection, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.DestinationTableName = tableName;
                foreach (IColumnMapping colMap in data.ColumnMapping)
                    bulkCopy.ColumnMappings.Add(colMap.SourceColumn, colMap.DataSetColumn);
                bulkCopy.WriteToServer(data);
            }
        }

        public override IConnectionManager Clone()
        {
            if (LeaveOpen) return this;

            var clone = new MsSqlConnectionManager(ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts,
                ModifyDBSettings = this.ModifyDBSettings
            };
            return clone;
        }
    }
}
