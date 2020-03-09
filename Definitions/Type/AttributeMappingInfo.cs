using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace z.ETL.DataFlow
{
    internal class AttributeMappingInfo
    {
        internal PropertyInfo PropInInput { get; set; }
        internal string PropNameInInput { get; set; }
        internal PropertyInfo PropInOutput { get; set; }
    }
}
