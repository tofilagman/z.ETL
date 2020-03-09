﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace z.ETL.ConnectionManager
{
    /// <summary>
    /// This class creates the necessary sql statements that simulate the missing bulk insert function in Odbc connections.
    /// Normally this will be a insert into with multiple values.
    /// For access databases this will differ.
    /// </summary>
    /// <see cref="OdbcConnectionManager"/>
    /// <see cref="AccessOdbcConnectionManager"/>
    internal class BulkInsertSql<T> where T : DbParameter, new()
    {
        internal bool IsAccessDatabase => ConnectionType == ConnectionManagerType.Access;
        internal bool UseParameterQuery { get; set; } = true;
        internal bool UseNamedParameters { get; set; }
        internal List<T> Parameters { get; set; }
        StringBuilder QueryText { get; set; }
        List<string> SourceColumnNames { get; set; }
        List<string> DestColumnNames { get; set; }
        internal string AccessDummyTableName { get; set; }
        internal ConnectionManagerType ConnectionType { get; set; }
        internal string QB => ConnectionManagerSpecifics.GetBeginQuotation(ConnectionType);
        internal string QE => ConnectionManagerSpecifics.GetEndQuotation(ConnectionType);
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, ConnectionType);
        internal string TableName { get; set; }
        private int ParameterNameCount { get; set; }

        internal string CreateBulkInsertStatement(ITableData data, string tableName)
        {
            InitObjects();
            TableName = tableName;
            GetSourceAndDestColumNames(data);
            AppendBeginSql(tableName);
            ReadDataAndCreateQuery(data);
            AppendEndSql();
            return QueryText.ToString();
        }

        private void InitObjects()
        {
            QueryText = new StringBuilder();
            Parameters = new List<T>();
        }

        private void GetSourceAndDestColumNames(ITableData data)
        {
            SourceColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            DestColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();
        }

        private void ReadDataAndCreateQuery(ITableData data)
        {
            while (data.Read())
            {
                List<string> values = new List<string>();
                foreach (string destColumnName in DestColumnNames)
                {
                    int colIndex = data.GetOrdinal(destColumnName);
                    if (data.IsDBNull(colIndex))
                        AddNullValue(values, destColumnName);
                    else
                        AddNonNullValue(data, values, destColumnName, colIndex);
                }
                AppendValueListSql(values, data.NextResult());
            }
        }

        private void AddNullValue(List<string> values, string destColumnName)
        {
            if (UseParameterQuery)
            {
                values.Add(CreateParameterWithValue(DBNull.Value));
            }
            else
            {
                string value = IsAccessDatabase ? $"NULL AS {destColumnName}" : "NULL";
                values.Add(value);
            }

        }

        private void AddNonNullValue(ITableData data, List<string> values, string destColumnName, int colIndex)
        {
            if (UseParameterQuery)
            {
                values.Add(CreateParameterWithValue(data.GetValue(colIndex)));
            }
            else
            {
                string value = data.GetString(colIndex).Replace("'", "''");
                string valueSql = IsAccessDatabase ? $"'{value}' AS {destColumnName}"
                    : $"'{value}'";
                values.Add(valueSql);
            }

        }

        private string CreateParameterWithValue(object parValue)
        {
            var par = new T();
            par.Value = parValue;
            Parameters.Add(par);
            if (UseNamedParameters)
            {
                string parName = $"@P{ParameterNameCount++}";
                par.ParameterName = parName;
                return parName;
            }
            else
            {
                return "?";
            }
        }

        private void AppendBeginSql(string tableName)
        {
            QueryText.AppendLine($@"INSERT INTO {TN.QuotatedFullName} ({string.Join(",", SourceColumnNames.Select(col => QB + col + QE))})");
            if (IsAccessDatabase)
                QueryText.AppendLine("  SELECT * FROM (");
            else
                QueryText.AppendLine("VALUES");
        }

        private void AppendValueListSql(List<string> values, bool lastItem)
        {
            if (IsAccessDatabase)
            {
                QueryText.AppendLine("SELECT " + string.Join(",", values) + $"  FROM {AccessDummyTableName} ");
                if (lastItem) QueryText.AppendLine(" UNION ALL ");
            }
            else
            {
                QueryText.Append("(" + string.Join(",", values) + $")");
                if (lastItem) QueryText.AppendLine(",");
            }
        }

        private void AppendEndSql()
        {
            if (IsAccessDatabase)
                QueryText.AppendLine(") a;");
        } 
    }
     
}
