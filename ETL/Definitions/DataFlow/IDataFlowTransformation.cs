using System;

namespace z.ETL.DataFlow
{
    public interface IDataFlowTransformation<TInput, TOutput> : IDataFlowLinkSource<TOutput>, IDataFlowLinkTarget<TInput>
    {

    }
}
