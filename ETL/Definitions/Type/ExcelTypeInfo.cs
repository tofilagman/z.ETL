﻿using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;

namespace z.ETL.DataFlow
{
    internal class ExcelTypeInfo : TypeInfo
    {
        internal Dictionary<int, int> ExcelIndex2PropertyIndex { get; set; } = new Dictionary<int, int>();

        internal ExcelTypeInfo(Type typ) : base(typ)
        {

        }

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            AddExcelColumnAttribute(propInfo, currentIndex);
        }

        private void AddExcelColumnAttribute(PropertyInfo propInfo, int curIndex)
        {
            var attr = propInfo.GetCustomAttribute(typeof(ExcelColumn)) as ExcelColumn;
            if (attr != null)
                ExcelIndex2PropertyIndex.Add(attr.Index, curIndex);
        }

        internal object CastPropertyValue(PropertyInfo property, string value)
        {
            if (property == null || String.IsNullOrEmpty(value))
                return null;
            if (property.PropertyType == typeof(bool))
                return value == "1" || value == "true" || value == "on" || value == "checked";
            else
            {
                Type t = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                return Convert.ChangeType(value, t);
            }
        }
    }
}

