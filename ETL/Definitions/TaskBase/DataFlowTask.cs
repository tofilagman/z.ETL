using z.ETL.DataFlow;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;

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

        public int MaxProgressCount { get; set; }
        public int ProgressCount { get; set; }
        public Action<int> OnProgress { get; set; }

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

        public void LogProgress()
        {
            ProgressCount += 1;
            if (OnProgress != null)
            {
                var hh = (Convert.ToDouble(ProgressCount) / Convert.ToDouble(MaxProgressCount)) * 100.0;
                OnProgress?.Invoke(Convert.ToInt32(hh));
            }

            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                Logger?.LogInformation(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }


    }

}
