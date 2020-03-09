using z.ETL.ConnectionManager;
using z.ETL.ControlFlow;

namespace z.ETL.Helper
{
    public class DatabaseHelper
    {
        private static void DropAndCreate(IConnectionManager connManagerMaster, string dbName)
        {
            new DropDatabaseTask(dbName)
            {
                DisableLogging = true,
                ConnectionManager = connManagerMaster
            }.DropIfExists();

            new CreateDatabaseTask(dbName)
            {
                DisableLogging = true,
                ConnectionManager = connManagerMaster
            }.Execute();
        } 
    }
}
