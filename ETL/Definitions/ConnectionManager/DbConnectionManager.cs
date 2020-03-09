﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace z.ETL.ConnectionManager
{
    public abstract class DbConnectionManager<Connection> : IDisposable, IConnectionManager
        where Connection : class, IDbConnection, new()
    {
        public int MaxLoginAttempts { get; set; } = 3;
        public bool LeaveOpen { get; set; } = false;

        public string ConnectionString { get; set; }

        internal Connection DbConnection { get; set; }
        public ConnectionState? State => DbConnection?.State; 
        public   ConnectionManagerType Type { get; private set; }

        public DbConnectionManager() { }

        public DbConnectionManager(string connectionString, ConnectionManagerType type) : this()
        {
            this.ConnectionString = connectionString;
            this.Type = type;
        }

        public void Open()
        {
            if (LeaveOpen)
            {
                if (DbConnection == null)
                {
                    DbConnection = new Connection
                    {
                        ConnectionString = ConnectionString
                    };
                }
            }
            else
            {
                DbConnection?.Close();
                DbConnection = new Connection
                {
                    ConnectionString = ConnectionString
                };
            }
            if (DbConnection.State != ConnectionState.Open)
            {
                TryOpenConnectionXTimes();
            }
        }

        private void TryOpenConnectionXTimes()
        {
            bool successfullyConnected = false;
            Exception lastException = null;
            for (int i = 1; i <= MaxLoginAttempts; i++)
            {
                try
                {
                    DbConnection.Open();
                    successfullyConnected = true;
                }
                catch (Exception e)
                {
                    successfullyConnected = false;
                    lastException = e;
                    Task.Delay(1000).Wait();
                }
                if (successfullyConnected)
                {
                    break;
                }
            }
            if (!successfullyConnected)
            {
                throw lastException ?? new Exception("Could not connect to database!");
            }
        }

        public IDbCommand CreateCommand(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            var cmd = DbConnection.CreateCommand();
            cmd.CommandTimeout = 0;
            cmd.CommandText = commandText;
            if (parameterList != null)
            {
                foreach (QueryParameter par in parameterList)
                {
                    var newPar = cmd.CreateParameter();
                    newPar.ParameterName = par.Name;
                    newPar.DbType = par.DBType;
                    newPar.Value = par.Value;
                    cmd.Parameters.Add(newPar);
                }
            }
            return cmd;
        }

        public int ExecuteNonQuery(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            IDbCommand sqlcmd = CreateCommand(commandText, parameterList);
            return sqlcmd.ExecuteNonQuery();
        }

        public object ExecuteScalar(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            return cmd.ExecuteScalar();
        }

        public IDataReader ExecuteReader(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            return cmd.ExecuteReader();

        }

        public abstract void BulkInsert(ITableData data, string tableName);
        public abstract void BeforeBulkInsert(string tableName);
        public abstract void AfterBulkInsert(string tableName);

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (DbConnection != null)
                        DbConnection.Close();
                    DbConnection = null;
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Close()
        {
            Dispose();
        }

        public abstract IConnectionManager Clone();
        #endregion

    }
}
