using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using LumenWorks.Framework.IO.Csv;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DataExportJob
{
    public class DataExportService
    {
        public async Task ProcessDataExportJob()
        {
            try
            {
                string sheetTableMappingCSVPath = ConfigurationManager.AppSettings["SheetTableMappingCSVPath"];

                if (File.Exists(sheetTableMappingCSVPath))
                {
                    var sheetTableMappingTable = new DataTable();
                    using (var sheetTableMappingReader = new CsvReader(new StreamReader(System.IO.File.OpenRead(sheetTableMappingCSVPath)), true))
                    {
                        sheetTableMappingTable.Load(sheetTableMappingReader);
                    }
                    List<SheetTableMappingModel> sheetTableMappingModels = new List<SheetTableMappingModel>();
                    var tasks = new List<Task>();
                    int failed = 0;

                    foreach (DataRow tableSheet in sheetTableMappingTable.Rows)
                    {
                        tasks.Add(Task.Run(() =>
                        {
                            SheetTableMappingModel sheetTableMappingModel = new SheetTableMappingModel();
                            try
                            {
                                sheetTableMappingModel.TableName = tableSheet["TableName"].ToString();
                                sheetTableMappingModel.SheetName = tableSheet["SheetName"].ToString();

                                sheetTableMappingModel.Data = GetTableDataAsync(sheetTableMappingModel.TableName).Result;
                                sheetTableMappingModels.Add(sheetTableMappingModel);
                            }
                            catch (Exception taskexe)
                            {
                                Interlocked.Increment(ref failed);
                                Log.error(string.Format("Error in ProcessDataExportJob for Table {0}, Sheet {1}. Error Message: {2}, Inner Exception :{3}", tableSheet["TableName"].ToString(), tableSheet["SheetName"].ToString(), taskexe.Message, taskexe.InnerException.Message), taskexe);
                                //throw;
                            }
                        }));
                    }

                    Task t = Task.WhenAll(tasks);
                    try
                    {
                        t.Wait();
                    }
                    catch { }

                    if (t.Status == TaskStatus.RanToCompletion)
                    {
                        Console.WriteLine("All sheetTableMappingCSVPath attempts succeeded.");
                        Log.info(string.Format("All sheetTableMappingCSVPath attempts succeeded"));


                    }
                    else if (t.Status == TaskStatus.Faulted)
                    {
                        Console.WriteLine("{0} sheetTableMappingCSVPath attempts failed", failed);
                        Log.info(string.Format("{0} sheetTableMappingCSVPath attempts failed", failed));
                    }

                    // await ExportDatasetOpenExcel(sheetTableMappingModels);
                    await CopyMasterAndExportDatasetOpenExcel(sheetTableMappingModels);
                }
                else
                {
                    new InvalidOperationException("sheetTableMappingCSVPath does not exists.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format("Error in ProcessDataExportJob. Error Message: {0}, Inner Exception :{1}", ex.Message, ex.InnerException.Message));
                Log.error(string.Format("Error in ProcessDataExportJob. Error Message: {0}, Inner Exception :{1}", ex.Message, ex.InnerException.Message), ex);
                throw ex;
            }
        }


        public async Task<DataTable> GetTableDataAsync(string tableName)
        {
            try
            {
                var table = new DataTable();
                using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["PPTLConnectionString"].ConnectionString))
                {
                    using (SqlDataAdapter SqlDataAdapter = new SqlDataAdapter("SELECT * FROM " + tableName, sqlConnection))
                    {
                        SqlDataAdapter.Fill(table);
                    }
                }
                return table;
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format("Error in GetTableDataAsync for {0}. Error Message: {1}, Inner Exception :{2}", tableName, ex.Message, ex.InnerException.Message));
                Log.error("Error in GetTableDataAsync", ex);
                throw ex;
            }
        }


        public async Task WriteDataToOpenExcel(List<SheetTableMappingModel> sheetTableMappingModels)
        {
            try
            {
                Utility utility = new Utility();
                List<Task> tasks = new List<Task>();
                string saveAsLocation = ConfigurationManager.AppSettings["UpdatedExcelPath"];
                //var datetime = DateTime.Now.ToString().Replace("/", "_").Replace(":", "_");

                //string fileFullname = Path.Combine(saveAsLocation, "Output.xlsx");

                if (File.Exists(saveAsLocation))
                {
                    //fileFullname = Path.Combine(OutPutFileDirectory, "Output_" + datetime + ".xlsx");
                    File.Delete(saveAsLocation);
                }

                // using (SpreadsheetDocument spreadSheet = SpreadsheetDocument.Open(saveAsLocation, true))
                using (SpreadsheetDocument spreadSheet = SpreadsheetDocument.Create(saveAsLocation, SpreadsheetDocumentType.Workbook))
                {
                    WorkbookPart workbookpart = spreadSheet.AddWorkbookPart();
                    workbookpart.Workbook = new Workbook();

                    foreach (SheetTableMappingModel sheetTableMappingModel in sheetTableMappingModels)
                    {
                        try
                        {
                            WorksheetPart worksheetPart = utility.InsertSheetPartByName(spreadSheet, sheetTableMappingModel.SheetName);
                            if (worksheetPart != null)
                            {
                                List<String> columns = new List<string>();
                                foreach (System.Data.DataColumn column in sheetTableMappingModel.Data.Columns)
                                {
                                    columns.Add(column.ColumnName);
                                }
                                Worksheet worksheet = worksheetPart.Worksheet;
                                SheetData sheetData = worksheet.GetFirstChild<SheetData>();

                                foreach (DataRow dtrow in sheetTableMappingModel.Data.Rows)
                                {
                                    try
                                    {
                                        Row newRow = new Row();
                                        foreach (String col in columns)
                                        {
                                            string objdtDataType = dtrow[col].GetType().ToString();
                                            Cell cell = new Cell();
                                            //cell.DataType = CellValues.String;
                                            //cell.CellValue = new CellValue(dtrow[col].ToString()); //

                                            //Add text to text cell
                                            if (objdtDataType.Contains(TypeCode.Int32.ToString()) || objdtDataType.Contains(TypeCode.Int64.ToString()) || objdtDataType.Contains(TypeCode.Decimal.ToString()))
                                            {
                                                cell.DataType = new EnumValue<CellValues>(CellValues.Number);
                                                cell.CellValue = new CellValue(dtrow[col].ToString());
                                            }
                                            else
                                            {
                                                cell.DataType = new EnumValue<CellValues>(CellValues.String);
                                                cell.CellValue = new CellValue(dtrow[col].ToString());
                                            }
                                            newRow.AppendChild(cell);
                                        }
                                        sheetData.AppendChild(newRow);
                                    }
                                    catch (Exception ex)
                                    {

                                    }
                                }
                                worksheetPart.Worksheet.Save();
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(string.Format("Error in worksheetPart. Error Message: {0}, Inner Exception :{1}", ex.Message, ex.InnerException));
                            Log.error("Error in worksheetPart", ex);

                        }
                    }
                }
                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
            }
            finally
            {

            }
        }


        public async Task ExportDatasetOpenExcel(List<SheetTableMappingModel> sheetTableMappingModels)
        {
            try
            {
                Utility utility = new Utility();
                List<Task> tasks = new List<Task>();
                string saveAsLocation = ConfigurationManager.AppSettings["UpdatedExcelPath"];
                //var datetime = DateTime.Now.ToString().Replace("/", "_").Replace(":", "_");
                //string fileFullname = Path.Combine(saveAsLocation, "Output.xlsx");

                if (File.Exists(saveAsLocation))
                {
                    //fileFullname = Path.Combine(OutPutFileDirectory, "Output_" + datetime + ".xlsx");
                    File.Delete(saveAsLocation);
                }

                // using (SpreadsheetDocument spreadSheet = SpreadsheetDocument.Open(saveAsLocation, true))
                using (SpreadsheetDocument spreadSheet = SpreadsheetDocument.Create(saveAsLocation, SpreadsheetDocumentType.Workbook))
                {

                    var workbookPart = spreadSheet.AddWorkbookPart();
                    spreadSheet.WorkbookPart.Workbook = new Workbook();
                    spreadSheet.WorkbookPart.Workbook.Sheets = new Sheets();
                    int failed = 0;

                    foreach (SheetTableMappingModel sheetTableMappingModel in sheetTableMappingModels)
                    {
                        //tasks.Add(Task.Run(() =>
                        //{
                            try
                            {
                                var sheetPart = spreadSheet.WorkbookPart.AddNewPart<WorksheetPart>();
                                var sheetData = new SheetData();
                                sheetPart.Worksheet = new Worksheet(sheetData);

                                Sheets sheets = spreadSheet.WorkbookPart.Workbook.GetFirstChild<Sheets>();
                                string relationshipId = spreadSheet.WorkbookPart.GetIdOfPart(sheetPart);

                                uint sheetId = 1;
                                if (sheets.Elements<Sheet>().Count() > 0)
                                {
                                    sheetId =
                                        sheets.Elements<Sheet>().Select(s => s.SheetId.Value).Max() + 1;
                                }

                                Sheet sheet = new Sheet() { Id = relationshipId, SheetId = sheetId, Name = sheetTableMappingModel.SheetName };
                                sheets.Append(sheet);

                                Row headerRow = new Row();

                                List<String> columns = new List<string>();
                                foreach (DataColumn column in sheetTableMappingModel.Data.Columns)
                                {
                                    columns.Add(column.ColumnName);

                                    Cell cell = new Cell();
                                    cell.DataType = CellValues.String;
                                    cell.CellValue = new CellValue(column.ColumnName);
                                    headerRow.AppendChild(cell);
                                }

                                sheetData.AppendChild(headerRow);

                                foreach (DataRow dsrow in sheetTableMappingModel.Data.Rows)
                                {
                                    Row newRow = new Row();
                                    foreach (String col in columns)
                                    {
                                        Cell cell = new Cell();
                                        cell.DataType = CellValues.String;
                                        cell.CellValue = new CellValue(dsrow[col].ToString()); //
                                        newRow.AppendChild(cell);
                                    }

                                    sheetData.AppendChild(newRow);
                                }
                            }
                            catch (Exception ex)
                            {
                              //  Interlocked.Increment(ref failed);
                                Console.WriteLine(string.Format("Error in worksheetPart. Error Message: {0}, Inner Exception :{1}", ex.Message, ex.InnerException));
                                Log.error("Error in worksheetPart", ex);

                            }
                        //}));
                    }

                    //Task t = Task.WhenAll(tasks);
                    //try
                    //{
                    //    t.Wait();
                    //}
                    //catch { }

                    //if (t.Status == TaskStatus.RanToCompletion)
                    //{
                    //    Console.WriteLine("All ExportDatasetOpenExcel attempts succeeded.");
                    //    Log.info(string.Format("All ExportDatasetOpenExcel attempts succeeded"));
                    //}
                    //else if (t.Status == TaskStatus.Faulted)
                    //{
                    //    Console.WriteLine("{0} ExportDatasetOpenExcel attempts failed", failed);
                    //    Log.info(string.Format("{0} ExportDatasetOpenExcel attempts failed", failed));
                    //}
                }
               // await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
            }
            finally
            {

            }
        }

        public async Task CopyMasterAndExportDatasetOpenExcel(List<SheetTableMappingModel> sheetTableMappingModels)
        {
            try
            {
                Utility utility = new Utility();
                List<Task> tasks = new List<Task>();
                //string saveAsLocation = ConfigurationManager.AppSettings["UpdatedExcelPath"];
                ////var datetime = DateTime.Now.ToString().Replace("/", "_").Replace(":", "_");
                ////string fileFullname = Path.Combine(saveAsLocation, "Output.xlsx");

                //if (File.Exists(saveAsLocation))
                //{
                //    //fileFullname = Path.Combine(OutPutFileDirectory, "Output_" + datetime + ".xlsx");
                //    File.Delete(saveAsLocation);
                //}

                string ExcelPath = ConfigurationManager.AppSettings["ExcelPath"];
                string saveAsLocation = ConfigurationManager.AppSettings["UpdatedExcelPath"];
                if (File.Exists(saveAsLocation))
                {
                    File.Delete(saveAsLocation);
                }
                File.Copy(ExcelPath, saveAsLocation, true);


                using (SpreadsheetDocument spreadSheet = SpreadsheetDocument.Open(saveAsLocation, true))
                {
                    

                    foreach (SheetTableMappingModel sheetTableMappingModel in sheetTableMappingModels)
                    {
                        try
                        {
                            WorksheetPart worksheetPart = utility.RetrieveSheetPartByName(spreadSheet, sheetTableMappingModel.SheetName);
                            if (worksheetPart != null)
                            {

                                
                                //int sheetIndex = 0;
                                // utility.AddUpdateCellValue(spreadSheet, "test sheet1", 8, "A", "test data1");

                                List<String> columns = new List<string>();
                                foreach (System.Data.DataColumn column in sheetTableMappingModel.Data.Columns)
                                {
                                    columns.Add(column.ColumnName);
                                }
                                Worksheet worksheet = worksheetPart.Worksheet;
                                SheetData sheetData = worksheet.GetFirstChild<SheetData>();

                                foreach (DataRow dtrow in sheetTableMappingModel.Data.Rows)
                                {
                                    try
                                    {
                                        Row newRow = new Row();
                                        foreach (String col in columns)
                                        {
                                            string objdtDataType = dtrow[col].GetType().ToString();
                                            Cell cell = new Cell();
                                            //cell.DataType = CellValues.String;
                                            //cell.CellValue = new CellValue(dtrow[col].ToString()); //

                                            //Add text to text cell
                                            if (objdtDataType.Contains(TypeCode.Int32.ToString()) || objdtDataType.Contains(TypeCode.Int64.ToString()) || objdtDataType.Contains(TypeCode.Decimal.ToString()))
                                            {
                                                cell.DataType = new EnumValue<CellValues>(CellValues.Number);
                                                cell.CellValue = new CellValue(dtrow[col].ToString());
                                            }
                                            else
                                            {
                                                cell.DataType = new EnumValue<CellValues>(CellValues.String);
                                                cell.CellValue = new CellValue(dtrow[col].ToString());
                                            }
                                            newRow.AppendChild(cell);
                                        }
                                        sheetData.AppendChild(newRow);
                                    }
                                    catch (Exception ex)
                                    {

                                    }
                                }

                                worksheetPart.Worksheet.Save();

                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(string.Format("Error in worksheetPart. Error Message: {0}, Inner Exception :{1}", ex.Message, ex.InnerException));
                            Log.error("Error in worksheetPart", ex);

                        }
                    }

                    //WorkbookPart wbPart = spreadSheet.WorkbookPart;
                    //Sheets theSheets = wbPart.Workbook.Sheets;
                    //foreach (OpenXmlElement sheet in theSheets)
                    //{
                    //    foreach (OpenXmlAttribute attr in sheet.GetAttributes())
                    //    {
                    //        Console.WriteLine("{0}: {1}", attr.LocalName, attr.Value);
                    //    }
                    //}
                    //int sheetIndex = 0;
                    //foreach (WorksheetPart excelSheet in spreadSheet.WorkbookPart.WorksheetParts)
                    //{
                    //    string Sheet2 = ConfigurationManager.AppSettings["Sheet2"];
                    //    string Sheet3 = ConfigurationManager.AppSettings["Sheet3"];
                    //    string Sheet10 = ConfigurationManager.AppSettings["Sheet10"];
                    //    string Sheet7 = ConfigurationManager.AppSettings["Sheet7"];

                    //    Log.info("ExcelSheet Name :" + excelSheet.Worksheet.XName);
                    //    Console.WriteLine("ExcelSheet Name :" + excelSheet.Worksheet.XName);
                    //    //if (excelSheet.Name == Sheet2 || excelSheet.Name == Sheet3 || excelSheet.Name == Sheet10 || excelSheet.Name == Sheet7)
                    //    //{
                    //    //    tasks.Add(ProcessExcel(dataMigrationService, excelSheet, Sheet2, Sheet3, Sheet10, Sheet7, xlWorkBook));
                    //    //}
                    //    sheetIndex++;
                    //}
                    //cell.CellValue = new CellValue(text);
                    //cell.DataType = new EnumValue<CellValues>(CellValues.SharedString);

                    // worksheetPart.Worksheet.Save();
                }

                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
            }
            finally
            {

            }
        }
    }

}
