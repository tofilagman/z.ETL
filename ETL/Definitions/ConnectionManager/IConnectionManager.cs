using System;
using System.Collections.Generic;
using System.Data;

namespace z.ETL.ConnectionManager
{
    public interface IConnectionManager : IDisposable
    {
        string ConnectionString { get; set; }
        void Open();
        void Close();
        IDbCommand CreateCommand(string commandText, IEnumerable<QueryParameter> parameterList);
        int ExecuteNonQuery(string command, IEnumerable<QueryParameter> parameterList = null);
        object ExecuteScalar(string command, IEnumerable<QueryParameter> parameterList = null);
        IDataReader ExecuteReader(string command, IEnumerable<QueryParameter> parameterList = null);
        void BulkInsert(ITableData data, string tableName);
        void BeforeBulkInsert(string tableName);
        void AfterBulkInsert(string tableName);
        IConnectionManager Clone();
        bool LeaveOpen { get; set; } 
        ConnectionManagerType Type { get; } 
    }
}
