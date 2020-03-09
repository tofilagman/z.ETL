using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Text;
using z.ETL;
using z.ETL.ConnectionManager;

namespace ETLTest.Connectors
{
    public class MySqlConnectionManager: DbConnectionManager<MySqlConnection>
    {
        public MySqlConnectionManager(string connectionString):
            base(connectionString, ConnectionManagerType.MySql)
        {

        }

        public override void AfterBulkInsert(string tableName)
        {
            
        }

        public override void BeforeBulkInsert(string tableName)
        {
          
        }

        public override void BulkInsert(ITableData data, string tableName)
        {
            BulkInsertSql<MySqlParameter> bulkInsert = new BulkInsertSql<MySqlParameter>()
            {
                UseParameterQuery = true,
                ConnectionType = ConnectionManagerType.MySql
            };
            string sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
            var cmd = DbConnection.CreateCommand();
            cmd.Parameters.AddRange(bulkInsert.Parameters.ToArray());
            cmd.CommandText = sql;
            cmd.Prepare();
            cmd.ExecuteNonQuery();
        }

        public override IConnectionManager Clone()
        {
            MySqlConnectionManager clone = new MySqlConnectionManager(ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
