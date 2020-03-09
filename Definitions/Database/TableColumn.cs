﻿using z.ETL.ConnectionManager;
using System;
using System.Data;

namespace z.ETL
{
    public class TableColumn : ITableColumn, IColumnMapping
    {
        private string _dataSetColumn;
        private string _sourceColumn;

        public string Name { get; set; }
        public string DataType { get; set; }
        internal string InternalDataType { get; set; }
        public bool AllowNulls { get; set; }
        public bool IsIdentity { get; set; }
        public int? IdentitySeed { get; set; }
        public int? IdentityIncrement { get; set; }
        public bool IsPrimaryKey { get; set; }
        public string DefaultValue { get; set; }
        public string Collation { get; set; }
        public string ComputedColumn { get; set; }
        public bool HasComputedColumn => !String.IsNullOrWhiteSpace(ComputedColumn);
        public System.Type NETDataType => Type.GetType(DataTypeConverter.GetNETObjectTypeString(DataType));


        public string DataSetColumn
        {
            get { return String.IsNullOrWhiteSpace(_dataSetColumn) ? Name : _dataSetColumn; }
            set
            {
                _dataSetColumn = value;
            }
        }
        public string SourceColumn
        {
            get { return String.IsNullOrWhiteSpace(_sourceColumn) ? Name : _sourceColumn; }
            set
            {
                _sourceColumn = value;
            }
        }

        public TableColumn() { }
        public TableColumn(string name, string dataType) : this()
        {
            Name = name;
            DataType = dataType;
        }

        public TableColumn(string name, string dataType, bool allowNulls) : this(name, dataType)
        {
            AllowNulls = allowNulls;
        }

        public TableColumn(string name, string dataType, bool allowNulls, bool isPrimaryKey) : this(name, dataType, allowNulls)
        {
            IsPrimaryKey = isPrimaryKey;
        }

        public TableColumn(string name, string dataType, bool allowNulls, bool isPrimaryKey, bool isIdentity) : this(name, dataType, allowNulls, isPrimaryKey)
        {
            IsIdentity = isIdentity;
        }
    }
}
