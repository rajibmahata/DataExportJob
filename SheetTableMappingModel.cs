using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataExportJob
{
    public class SheetTableMappingModel
    {
        public SheetTableMappingModel()
        {
            Data = new DataTable();
        }
        public string SheetName { get; set; }
        public string TableName { get; set; }
        public DataTable Data { get; set; }
    }
}
