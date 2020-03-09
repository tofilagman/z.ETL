﻿using z.ETL.ConnectionManager;
using z.ETL.ControlFlow;
using z.ETL.Helper;
using System;
using System.Collections.Generic;

namespace z.ETL.Logging
{
    /// <summary>
    /// Will set the table entry for current load process to aborted.
    /// </summary>
    public class AbortLoadProcessTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Abort process with key {LoadProcessId}";
        public void Execute()
        {
            QueryParameter cd = new QueryParameter("CurrentDate", "DATETIME", DateTime.Now);
            QueryParameter em = new QueryParameter("AbortMessage", "VARCHAR(100)", AbortMessage);
            QueryParameter lpk = new QueryParameter("LoadProcessId", "BIGINT", LoadProcessId);
            new SqlTask(this, Sql)
            {
                DisableLogging = true,
                Parameter = new List<QueryParameter>() { cd, em, lpk },
            }.ExecuteNonQuery();
            var rlp = new ReadLoadProcessTableTask(this, LoadProcessId)
            {
                DisableLogging = true,
            };
            rlp.Execute();
            ControlFlow.ControlFlow.CurrentLoadProcess = rlp.LoadProcess;
        }

        /* Public properties */
        public long? _loadProcessId;
        public long? LoadProcessId
        {
            get
            {
                return _loadProcessId ?? ControlFlow.ControlFlow.CurrentLoadProcess?.Id;
            }
            set
            {
                _loadProcessId = value;
            }
        }
        public string AbortMessage { get; set; }


        public string Sql => $@"
 UPDATE { TN.QuotatedFullName } 
  SET end_date = @CurrentDate
  , is_running = 0
  , was_successful = 0
  , was_aborted = 1
  , abort_message = @AbortMessage
  WHERE id = @LoadProcessId
";

        ObjectNameDescriptor TN => new ObjectNameDescriptor(ControlFlow.ControlFlow.LoadProcessTable, this.ConnectionType);


        public AbortLoadProcessTask()
        {

        }

        public AbortLoadProcessTask(long? loadProcessId) : this()
        {
            this.LoadProcessId = loadProcessId;
        }
        public AbortLoadProcessTask(long? loadProcessId, string abortMessage) : this(loadProcessId)
        {
            this.AbortMessage = abortMessage;
        }

        public AbortLoadProcessTask(string abortMessage) : this()
        {
            this.AbortMessage = abortMessage;
        }

        public static void Abort() => new AbortLoadProcessTask().Execute();
        public static void Abort(long? loadProcessId) => new AbortLoadProcessTask(loadProcessId).Execute();
        public static void Abort(string abortMessage) => new AbortLoadProcessTask(abortMessage).Execute();
        public static void Abort(long? loadProcessId, string abortMessage) => new AbortLoadProcessTask(loadProcessId, abortMessage).Execute();
        public static void Abort(IConnectionManager connectionManager)
            => new AbortLoadProcessTask() { ConnectionManager = connectionManager }.Execute();
        public static void Abort(IConnectionManager connectionManager, long? loadProcessId)
            => new AbortLoadProcessTask(loadProcessId) { ConnectionManager = connectionManager }.Execute();
        public static void Abort(IConnectionManager connectionManager, string abortMessage)
            => new AbortLoadProcessTask(abortMessage) { ConnectionManager = connectionManager }.Execute();
        public static void Abort(IConnectionManager connectionManager, long? loadProcessId, string abortMessage)
            => new AbortLoadProcessTask(loadProcessId, abortMessage) { ConnectionManager = connectionManager }.Execute();


    }
}
