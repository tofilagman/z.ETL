﻿using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks.Dataflow;
using CF = z.ETL.ControlFlow;

namespace z.ETL.DataFlow
{
    public class DataFlowLinker<TOutput>
    {
        public ISourceBlock<TOutput> SourceBlock { get; set; }
        public bool DisableLogging => CallingTask.DisableLogging;
        public ILogger Logger { get; set; }
        public DataFlowTask CallingTask { get; set; }

        public DataFlowLinker(DataFlowTask callingTask, ISourceBlock<TOutput> sourceBlock)
        {
            this.CallingTask = callingTask;
            this.SourceBlock = sourceBlock;
        }

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target)
            => LinkTo<TOutput>(target);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target)
        {
            SourceBlock.LinkTo(target.TargetBlock);
            target.AddPredecessorCompletion(SourceBlock.Completion);
            if (!DisableLogging)
                Logger?.LogDebug(CallingTask.TaskName + $" was linked to: {((ITask)target).TaskName}", CallingTask.TaskType, "LOG", CallingTask.TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            return target as IDataFlowLinkSource<TConvert>;
        }

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => LinkTo<TOutput>(target, predicate);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
        {
            SourceBlock.LinkTo(target.TargetBlock, predicate);
            target.AddPredecessorCompletion(SourceBlock.Completion);
            if (!DisableLogging)
                Logger?.LogDebug(CallingTask.TaskName + $" was linked to (with predicate): {((ITask)target).TaskName}!", CallingTask.TaskType, "LOG", CallingTask.TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            return target as IDataFlowLinkSource<TConvert>;
        }

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => LinkTo<TOutput>(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
        {
            SourceBlock.LinkTo(target.TargetBlock, rowsToKeep);
            target.AddPredecessorCompletion(SourceBlock.Completion);
            if (!DisableLogging)
                Logger?.LogDebug(CallingTask.TaskName + $" was linked to (with predicate): {((ITask)target).TaskName}!", CallingTask.TaskType, "LOG", CallingTask.TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);

            VoidDestination<TOutput> voidTarget = new VoidDestination<TOutput>();
            SourceBlock.LinkTo<TOutput>(voidTarget.TargetBlock, rowsIntoVoid);
            voidTarget.AddPredecessorCompletion(SourceBlock.Completion);
            if (!DisableLogging)
                Logger?.LogDebug(CallingTask.TaskName + $" was also linked to: VoidDestination to ignore certain rows!", CallingTask.TaskType, "LOG", CallingTask.TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);

            return target as IDataFlowLinkSource<TConvert>;
        }
    }
}
