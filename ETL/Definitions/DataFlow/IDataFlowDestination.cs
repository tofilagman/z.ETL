using System.Threading.Tasks;

namespace z.ETL.DataFlow
{
    public interface IDataFlowDestination<TInput> : IDataFlowLinkTarget<TInput>
    {
        void Wait();
        Task Completion { get; }
    }
}
