﻿using z.ETL.ConnectionManager;
using System;
using System.Linq;

namespace z.ETL.ControlFlow.SqlServer
{
    /// <summary>
    /// This task can exeucte any XMLA.
    /// </summary>
    /// <example>
    /// <code>
    /// XmlaTask.ExecuteNonQuery("Log description here","Xmla goes here...")
    /// </code>
    /// </example>
    public class XmlaTask : DbTask
    {
        public override string TaskName { get; set; } = "Run some xmla";

        public XmlaTask()
        {
            Init();
        }

        public XmlaTask(string name) : base(name)
        {
            Init();
        }

        internal XmlaTask(ITask callingTask, string xmla) : base(callingTask, xmla)
        {
            Init();
        }

        public XmlaTask(string name, string xmla) : base(name, xmla)
        {
            Init();
        }

        public XmlaTask(string name, string xmla, params Action<object>[] actions) : base(name, xmla, actions)
        {
            Init();
        }

        public XmlaTask(string name, string xmla, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) : base(name, xmla, beforeRowReadAction, afterRowReadAction, actions)
        {
            Init();
        }

        private void Init()
        {
            DoXMLCommentStyle = true;
        }

        /* Static methods for convenience */
        public static int ExecuteNonQuery(string name, string xmla) => new XmlaTask(name, xmla).ExecuteNonQuery();
        public static object ExecuteScalar(string name, string xmla) => new XmlaTask(name, xmla).ExecuteScalar();
        public static Nullable<T> ExecuteScalar<T>(string name, string xmla) where T : struct => new XmlaTask(name, xmla).ExecuteScalar<T>();
        public static bool ExecuteScalarAsBool(string name, string xmla) => new XmlaTask(name, xmla).ExecuteScalarAsBool();
        public static void ExecuteReaderSingleLine(string name, string xmla, params Action<object>[] actions) =>
           new XmlaTask(name, xmla, actions) { Limit = 1 }.ExecuteReader();
        public static void ExecuteReader(string name, string xmla, params Action<object>[] actions) => new XmlaTask(name, xmla, actions).ExecuteReader();
        public static void ExecuteReader(string name, string xmla, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new XmlaTask(name, xmla, beforeRowReadAction, afterRowReadAction, actions).ExecuteReader();
        public static int ExecuteNonQuery(IConnectionManager connectionManager, string name, string xmla) => new XmlaTask(name, xmla) { ConnectionManager = connectionManager }.ExecuteNonQuery();
        public static object ExecuteScalar(IConnectionManager connectionManager, string name, string xmla) => new XmlaTask(name, xmla) { ConnectionManager = connectionManager }.ExecuteScalar();
        public static Nullable<T> ExecuteScalar<T>(IConnectionManager connectionManager, string name, string xmla) where T : struct => new XmlaTask(name, xmla) { ConnectionManager = connectionManager }.ExecuteScalar<T>();
        public static bool ExecuteScalarAsBool(IConnectionManager connectionManager, string name, string xmla) => new XmlaTask(name, xmla) { ConnectionManager = connectionManager }.ExecuteScalarAsBool();
        public static void ExecuteReader(IConnectionManager connectionManager, string name, string xmla, params Action<object>[] actions) => new XmlaTask(name, xmla, actions) { ConnectionManager = connectionManager }.ExecuteReader();
        public static void ExecuteReader(IConnectionManager connectionManager, string name, string xmla, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new XmlaTask(name, xmla, beforeRowReadAction, afterRowReadAction, actions) { ConnectionManager = connectionManager }.ExecuteReader();
    }
}
