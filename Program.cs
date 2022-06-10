using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataExportJob
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("*** DataExportJob Service Started***");
            Log.info("***DataExportJob Service Started***");
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            DataExportService dataExportService = new DataExportService();
             dataExportService.ProcessDataExportJob();

            watch.Stop();

            Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine("***DataExportJob Service End***");
            Log.info("***DataExportJob Service End***");
        }
    }
}
