﻿using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace z.ETL.DataFlow
{
    public abstract class DataFlowBatchDestination<TInput> : DataFlowDestination<TInput[]>, ITask, IDataFlowDestination<TInput>
    {
        public Func<TInput[], TInput[]> BeforeBatchWrite { get; set; }
        public new ITargetBlock<TInput> TargetBlock => Buffer;
        public int BatchSize
        {
            get { return batchSize; }
            set
            {
                batchSize = value;
                InitObjects(batchSize);
            }
        }
        private int batchSize;

        public new void AddPredecessorCompletion(Task completion)
        {
            PredecessorCompletions.Add(completion);
            completion.ContinueWith(t => CheckCompleteAction());
        }

        protected new void CheckCompleteAction()
        {
            Task.WhenAll(PredecessorCompletions).ContinueWith(t =>
            {
                if (!TargetBlock.Completion.IsCompleted)
                {
                    if (t.IsFaulted) TargetBlock.Fault(t.Exception.InnerException);
                    else TargetBlock.Complete();
                }
            });
        }

        protected BatchBlock<TInput> Buffer { get; set; }

        protected virtual void InitObjects(int batchSize)
        {
            Buffer = new BatchBlock<TInput>(batchSize);
            TargetAction = new ActionBlock<TInput[]>(d => WriteBatch(ref d));
            SetCompletionTask();
            Buffer.LinkTo(TargetAction, new DataflowLinkOptions() { PropagateCompletion = true });
        }

        protected virtual void WriteBatch(ref TInput[] data)
        {
            if (ProgressCount == 0) NLogStart();
            if (BeforeBatchWrite != null)
                data = BeforeBatchWrite.Invoke(data);
        }
    }
}
