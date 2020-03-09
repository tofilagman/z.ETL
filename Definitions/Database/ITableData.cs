﻿using System;
using System.Collections.Generic;
using System.Data;

namespace z.ETL
{

    public interface ITableData : IDisposable, IDataReader
    {
        IColumnMappingCollection ColumnMapping { get; }
        List<object[]> Rows { get; }
    }
}
