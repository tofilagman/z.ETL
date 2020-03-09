using ETLTest.Connectors;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using z.ETL;
using z.ETL.ControlFlow;
using z.ETL.DataFlow;

namespace ETLTest
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestDataFlow()
        {
            var sourceCon = new MySqlConnectionManager("Server=10.37.128.2;Database=ETLBox_ControlFlow;Uid=etlbox;Pwd=etlboxpassword;");
            var destCon = new MsSqlConnectionManager("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;");

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(sourceCon, "SourceTable");
            RowTransformation<MySimpleRow, MySimpleRow> trans = new RowTransformation<MySimpleRow, MySimpleRow>(
                myRow => {
                    myRow.Value += 1;
                    return myRow;
                });
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(destCon, "DestinationTable");

            source.LinkTo(trans);
            trans.LinkTo(dest);

            source.Execute();
            dest.Wait();
        }

        [TestMethod]
        public void TestControlFlow()
        {
            var conn = new MsSqlConnectionManager("Server=10.37.128.2;Database=ETLBox_ControlFlow;Uid=etlbox;Pwd=etlboxpassword;");
            //Execute some Sql
            SqlTask.ExecuteNonQuery(conn, "Do some sql", $@"EXEC myProc");
            //Count rows
            int count = RowCountTask.Count(conn, "demo.table1").Value;
            //Create a table
            CreateTableTask.Create(conn, "Table1", new List<TableColumn>() {
                new TableColumn(name:"key",dataType:"INT",allowNulls:false,isPrimaryKey:true, isIdentity:true),
                new TableColumn(name:"value", dataType:"NVARCHAR(100)",allowNulls:true)
            });
        }
    }

    public class MySimpleRow
    {
        public string Column1 { get; set; }
        public int Value { get; set; }
    }
}
