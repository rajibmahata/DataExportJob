using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataExportJob
{
    public class Utility
    {
        public void AddUpdateCellValue(SpreadsheetDocument spreadSheet, string sheetname,
 uint rowIndex, string columnName, string text)
        {
            // Opening document for editing            
            WorksheetPart worksheetPart =
             RetrieveSheetPartByName(spreadSheet, sheetname);
            if (worksheetPart != null)
            {
                Cell cell = InsertCellInSheet(columnName, (rowIndex + 1), worksheetPart);
                cell.CellValue = new CellValue(text);
                //cell datatype            
                cell.DataType =
                 new EnumValue<CellValues>(CellValues.String);
                // Save the worksheet.            
                worksheetPart.Worksheet.Save();
            }
        }

        //retrieve sheetpart            
        public WorksheetPart InsertSheetPartByName(SpreadsheetDocument document,
         string sheetName)
        {
            //IEnumerable<Sheet> sheets =
            // document.WorkbookPart.Workbook.GetFirstChild<Sheets>().
            //Elements<Sheet>().Where(s => s.Name == sheetName);
            //if (sheets.Count() == 0)
            //    return null;

            //string relationshipId = sheets.First().Id.Value;
            //WorksheetPart worksheetPart = (WorksheetPart)
            //document.WorkbookPart.GetPartById(relationshipId);

            // Add a blank WorksheetPart.
            // Add a WorkbookPart to the document.
            //WorkbookPart workbookpart = document.AddWorkbookPart();
            //workbookpart.Workbook = new Workbook();
            WorksheetPart newWorksheetPart = document.WorkbookPart.AddNewPart<WorksheetPart>();
            newWorksheetPart.Worksheet = new Worksheet(new SheetData());

            Sheets sheets = document.WorkbookPart.Workbook.
            AppendChild<Sheets>(new Sheets());

           // Sheets sheets = document.WorkbookPart.Workbook.GetFirstChild<Sheets>();
            string relationshipId = document.WorkbookPart.GetIdOfPart(newWorksheetPart);

            // Get a unique ID for the new worksheet.
            uint sheetId = 1;
            if (sheets!=null && sheets.Elements<Sheet>().Count() > 0)
            {
                sheetId = sheets.Elements<Sheet>().Select(s => s.SheetId.Value).Max() + 1;
            }


            // Append the new worksheet and associate it with the workbook.
            Sheet sheet = new Sheet() { Id = relationshipId, SheetId = sheetId, Name = sheetName };
            sheets.Append(sheet);
           // workbookpart.Workbook.Save();

            return newWorksheetPart;
        }
        //retrieve sheetpart            
        public WorksheetPart RetrieveSheetPartByName(SpreadsheetDocument document,
         string sheetName)
        {
            IEnumerable<Sheet> sheets =
             document.WorkbookPart.Workbook.GetFirstChild<Sheets>().
            Elements<Sheet>().Where(s => s.Name == sheetName);
            if (sheets.Count() == 0)
                return null;

            string relationshipId = sheets.First().Id.Value;
            WorksheetPart worksheetPart = (WorksheetPart)
            document.WorkbookPart.GetPartById(relationshipId);
            return worksheetPart;
        }

        //insert cell in sheet based on column and row index            
        public Cell InsertCellInSheet(string columnName, uint rowIndex, WorksheetPart worksheetPart)
        {
            Worksheet worksheet = worksheetPart.Worksheet;
            SheetData sheetData = worksheet.GetFirstChild<SheetData>();
            string cellReference = columnName + rowIndex;
            Row row;
            //check whether row exist or not            
            //if row exist            
            if (sheetData.Elements<Row>().Where(r => r.RowIndex == rowIndex).Count() != 0)
                row = sheetData.Elements<Row>().Where(r => r.RowIndex == rowIndex).First();
            //if row does not exist then it will create new row            
            else
            {
                row = new Row()
                {
                    RowIndex = rowIndex
                };
                sheetData.Append(row);
            }
            //check whether cell exist or not            
            //if cell exist            
            if (row.Elements<Cell>().Where(c => c.CellReference.Value == columnName + rowIndex).Count() > 0)
                return row.Elements<Cell>().Where(c => c.CellReference.Value == cellReference).First();
            //if cell does not exist            
            else
            {
                Cell refCell = null;
                foreach (Cell cell in row.Elements<Cell>())
                {
                    if (string.Compare(cell.CellReference.Value, cellReference, true) > 0)
                    {
                        refCell = cell;
                        break;
                    }
                }
                Cell newCell = new Cell()
                {
                    CellReference = cellReference
                };
                row.InsertBefore(newCell, refCell);
                worksheet.Save();
                return newCell;
            }
        }

        // retrieve cell based on column and row index            
        public Cell RetreiveCell(Worksheet worksheet,
         string columnName, uint rowIndex)
        {
            Row row = RetrieveRow(worksheet, rowIndex);
            var newRow = new Row()
            {
                RowIndex = (uint)rowIndex + 1
            };
            //adding new row            
            worksheet.InsertAt(newRow, Convert.ToInt32(rowIndex + 1));
            //create cell with value            
            Cell cell = new Cell();
            cell.CellValue = new CellValue("");
            cell.DataType =
             new EnumValue<CellValues>(CellValues.String);
            newRow.AddAnnotation(cell);
            worksheet.Save();

            row = newRow;
            if (row == null)
                return null;
            return row.Elements<Cell>().Where(c => string.Compare(c.CellReference.Value, columnName +
         (rowIndex + 1), true) == 0).First();
        }

        // it will return a row based on worksheet and rowindex            
        public Row RetrieveRow(Worksheet worksheet, uint rowIndex)
        {
            return worksheet.GetFirstChild<SheetData>().
            Elements<Row>().Where(r => r.RowIndex == rowIndex).First();
        }

        public void DeleteCellInSheet(WorksheetPart worksheetPart)
        {
            // Get all the rows in the workbook
            IEnumerable<Row> rows = worksheetPart.Worksheet.GetFirstChild<SheetData>().Elements<Row>().Where(r => r.RowIndex > 1);
            // IEnumerable<Row> rows = worksheetPart.Worksheet.Descendants<Row>().Where(r => r.RowIndex > 1);
            // Ensure that there are actually rows in the workbook
            if (rows.Count() == 0)
            {
                return;
            }

            // Select all the cells from each row where the column letter is equal to index
            foreach (Row row in rows)
            {
                //var cellsToRemove = row.Elements<Cell>().Where(x => new String(x.CellReference.Value.Where(Char.IsLetter).ToArray()) == columnName);

                //foreach (var cell in cellsToRemove)
                //    cell.Remove();
                row.Remove();
            }
            worksheetPart.Worksheet.Save();
        }

    }
}
