﻿using System;

namespace z.ETL.DataFlow
{
    /// <summary>
    /// The generic ETLBox Exception. See inner exception for more details.
    /// </summary>
    public class ETLBoxError
    {
        public string ErrorText { get; set; }
        public DateTime ReportTime { get; set; }
        public Exception Exception { get; set; }
        public string RecordAsJson { get; set; }
    }
}
