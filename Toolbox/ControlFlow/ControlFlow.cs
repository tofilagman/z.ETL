﻿using z.ETL.ConnectionManager;
using z.ETL.Logging;

namespace z.ETL.ControlFlow
{
    /// <summary>
    /// Contains static information which affects all ETLBox tasks.
    /// Here you can set default connections string, disbale the logging for all processes or set the current stage used in your logging configuration.
    /// </summary>
    public static class ControlFlow
    {
        private static IConnectionManager _defaultDbConnection;
        /// <summary>
        /// You can store your general database connection string here. This connection will then used by all Tasks where no DB connection is excplicitly set.
        /// </summary>
        public static IConnectionManager DefaultDbConnection
        {
            get
            {
                if (_defaultDbConnection == null)
                    throw new ETLBoxException("No connection manager found! The component or task you are " +
                        "using expected a  connection manager to connect to the database." +
                        "Either pass a connection manager or set a default connection manager within the " +
                        "ControlFlow.DefaultDbConnection property!");
                return _defaultDbConnection;
            }
            set
            {
                _defaultDbConnection = value;
            }
        }

        /// <summary>
        /// If set to true, nothing will be logged by any control flow task or data flow component.
        /// When switched back to false, all tasks and components will continue to log.
        /// </summary>
        public static bool DisableAllLogging { get; set; }

        /// <summary>
        /// For logging purposes only. If the stage is set, you can access the stage value in the logging configuration.
        /// </summary>
        public static string STAGE { get; set; }

        /// <summary>
        /// If you used the logging task StartLoadProces (and created the corresponding load process table before)
        /// then this Property will hold the current load process information.
        /// </summary>
        public static LoadProcess CurrentLoadProcess { get; internal set; }

        public const string DEFAULTLOADPROCESSTABLENAME = "etlbox_loadprocess";
        /// <summary>
        /// TableName of the current load process logging table
        /// </summary>
        public static string LoadProcessTable { get; set; } = DEFAULTLOADPROCESSTABLENAME;

        public const string DEFAULTLOGTABLENAME = "etlbox_log";

        /// <summary>
        /// TableName of the current log process logging table
        /// </summary>
        public static string LogTable { get; set; } = DEFAULTLOGTABLENAME;
         
        /// <summary>
        /// Set all settings back to default (which is null or false)
        /// </summary>
        public static void ClearSettings()
        {
            DefaultDbConnection = null;
            CurrentLoadProcess = null;
            DisableAllLogging = false;
            LoadProcessTable = DEFAULTLOADPROCESSTABLENAME;
            LogTable = DEFAULTLOGTABLENAME;
            STAGE = null;
        }
    }



}
