﻿namespace z.ETL.ControlFlow
{
    public abstract class IfExistsTask : GenericTask, ITask
    {
        public override string TaskName => $"Check if {ObjectName} exists";
        public void Execute()
        {
            if (Sql != string.Empty)
                DoesExist = new SqlTask(this, Sql).ExecuteScalarAsBool();
        }

        public string ObjectName { get; set; }
        public ObjectNameDescriptor ON => new ObjectNameDescriptor(ObjectName, this.ConnectionType);
        internal string OnObjectName { get; set; }
        public ObjectNameDescriptor OON => new ObjectNameDescriptor(OnObjectName, this.ConnectionType);
        public bool DoesExist { get; internal set; }

        public string Sql
        {
            get
            {
                return GetSql();
            }
        }

        internal virtual string GetSql()
        {
            return string.Empty;
        }

        public virtual bool Exists()
        {
            Execute();
            return DoesExist;
        }
    }
}