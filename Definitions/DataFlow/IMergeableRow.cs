using System;

namespace z.ETL.DataFlow
{
    public interface IMergeableRow
    {
        DateTime ChangeDate { get; set; }
        string ChangeAction { get; set; }
        string UniqueId { get; }
        bool IsDeletion { get; }
    }
}
