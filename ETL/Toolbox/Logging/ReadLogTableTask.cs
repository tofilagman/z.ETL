﻿using z.ETL.ConnectionManager;
using z.ETL.ControlFlow;
using System;
using System.Collections.Generic;

namespace z.ETL.Logging
{
    /// <summary>
    /// Reads data from the etl.Log table.
    /// </summary>
    public class ReadLogTableTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Read all log entries for {LoadProcessId ?? 0 }";
        public void Execute()
        {
            LogEntries = new List<LogEntry>();
            LogEntry current = new LogEntry();
            new SqlTask(this, Sql)
            {
                DisableLogging = true,
                ConnectionManager = this.ConnectionManager,
                BeforeRowReadAction = () => current = new LogEntry(),
                AfterRowReadAction = () => LogEntries.Add(current),
                Actions = new List<Action<object>>() {
                    col => current.Id = Convert.ToInt64(col),
                    col => current.LogDate = (DateTime)col,
                    col => current.Level = (string)col,
                    col => current.Message = (string)col,
                    col => current.TaskType = (string)col,
                    col => current.TaskAction = (string)col,
                    col => current.TaskHash = (string)col,
                    col => current.Stage = (string)col,
                    col => current.Source = (string)col,
                    col => current.LoadProcessId = Convert.ToInt64(col),
                }
            }.ExecuteReader();
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

        public ReadLogTableTask ReadLog()
        {
            Execute();
            return this;
        }

        public List<LogEntry> LogEntries { get; private set; }

        public string Sql => $@"
SELECT {QB}id{QE}, {QB}log_date{QE}, {QB}level{QE}, {QB}message{QE}, {QB}task_type{QE}, {QB}task_action{QE}, {QB}task_hash{QE}, {QB}stage{QE}, {QB}source{QE}, {QB}load_process_id{QE}
FROM { TN.QuotatedFullName}" +
            (LoadProcessId != null ? $@" WHERE {QB}LoadProcessKey{QE} = {LoadProcessId}"
            : "");

        ObjectNameDescriptor TN => new ObjectNameDescriptor(ControlFlow.ControlFlow.LogTable, this.ConnectionType);

        public ReadLogTableTask()
        {

        }

        public ReadLogTableTask(long? loadProcessKey) : this()
        {
            this.LoadProcessId = loadProcessKey;
        }

        public static List<LogEntry> Read() => new ReadLogTableTask().ReadLog().LogEntries;
        public static List<LogEntry> Read(long? loadProcessId) => new ReadLogTableTask(loadProcessId).ReadLog().LogEntries;
        public static List<LogEntry> Read(IConnectionManager connectionManager)
            => new ReadLogTableTask() { ConnectionManager = connectionManager }.ReadLog().LogEntries;
        public static List<LogEntry> Read(IConnectionManager connectionManager, long? loadProcessId)
            => new ReadLogTableTask(loadProcessId) { ConnectionManager = connectionManager }.ReadLog().LogEntries;

    }
}
