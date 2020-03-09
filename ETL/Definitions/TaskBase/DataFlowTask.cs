using z.ETL.DataFlow;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace z.ETL
{
    public abstract class DataFlowTask : GenericTask, ITask
    {
        protected int? _loggingThresholdRows;
        public virtual int? LoggingThresholdRows
        {
            get
            {
                if (DataFlow.DataFlow.HasLoggingThresholdRows)
                    return DataFlow.DataFlow.LoggingThresholdRows;
                else
                    return _loggingThresholdRows;
            }
            set
            {
                _loggingThresholdRows = value;
            }
        }

        public int ProgressCount { get; set; }

        protected bool HasLoggingThresholdRows => LoggingThresholdRows != null && LoggingThresholdRows > 0;
        protected int ThresholdCount { get; set; } = 1;


        protected void NLogStart()
        {
            if (!DisableLogging)
                Logger?.LogInformation(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        protected void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                Logger?.LogInformation(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            if (!DisableLogging)
                Logger?.LogInformation(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        protected void LogProgressBatch(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && ProgressCount >= (LoggingThresholdRows * ThresholdCount))
            {
                Logger?.LogInformation(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
                ThresholdCount++;
            }
        }

        protected void LogProgress()
        {
            ProgressCount += 1;
            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                Logger?.LogInformation(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }


    }

}
