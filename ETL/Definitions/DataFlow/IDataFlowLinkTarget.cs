using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace z.ETL.DataFlow
{
    public interface IDataFlowLinkTarget<TInput> : ITask
    {
        ITargetBlock<TInput> TargetBlock { get; }
        void AddPredecessorCompletion(Task completion);
    }
}
