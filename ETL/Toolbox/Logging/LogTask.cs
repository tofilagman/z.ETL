using z.ETL.ConnectionManager;
using Microsoft.Extensions.Logging;

namespace z.ETL.Logging
{
    /// <summary>
    /// Used this task for custom log messages.
    /// </summary>
    public class LogTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Logs message";
        public void Execute()
        {
            Info(Message);
        }

        /* Public properties */
        public string Message { get; set; }

        public LogTask()
        {
        }

        public LogTask(string message) : this()
        {
            Message = message;
        }
        //NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Trace() => Logger?.LogTrace(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        public void Debug() =>  Logger?.LogDebug(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        public void Info() =>  Logger?.LogInformation(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        public void Warn() =>  Logger?.LogWarning(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        public void Error() =>  Logger?.LogError(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        
        public static void Trace(string message) => new LogTask(message).Trace();
        public static void Debug(string message) => new LogTask(message).Debug();
        public static void Info(string message) => new LogTask(message).Info();
        public static void Warn(string message) => new LogTask(message).Warn();
        public static void Error(string message) => new LogTask(message).Error(); 
        public static void Trace(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Trace();
        public static void Debug(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Debug();
        public static void Info(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Info();
        public static void Warn(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Warn();
        public static void Error(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Error();
    }
}
