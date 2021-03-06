/** @OnlyCurrentDoc */

function feature_importance() {
  var spreadsheet = SpreadsheetApp.getActive();
  var sheet = spreadsheet.getActiveSheet();
  sheet.getRange(1, 1, sheet.getMaxRows(), sheet.getMaxColumns()).activate();
  spreadsheet.setActiveSheet(spreadsheet.getSheetByName('Sheet1'), true);
  spreadsheet.getRange('A4').activate();
  spreadsheet.getRange('A1:II3').copyTo(spreadsheet.getActiveRange(), SpreadsheetApp.CopyPasteType.PASTE_NORMAL, true);
  spreadsheet.getRange('1:3').activate();
  spreadsheet.setCurrentCell(spreadsheet.getRange('A3'));
  spreadsheet.getActiveSheet().deleteRows(spreadsheet.getActiveRange().getRow(), spreadsheet.getActiveRange().getNumRows());
  spreadsheet.getRange('A1').activate();
  var currentCell = spreadsheet.getCurrentCell();
  spreadsheet.getActiveRange().getDataRegion().activate();
  currentCell.activateAsCurrentCell();
  spreadsheet.getRange('B1:C1').activate();
  spreadsheet.getActiveRangeList().setFontWeight('bold');
  spreadsheet.getRange('A1').activate();
  currentCell = spreadsheet.getCurrentCell();
  spreadsheet.getActiveRange().getDataRegion().activate();
  currentCell.activateAsCurrentCell();
  spreadsheet.getRange('A1:C243').createFilter();
  spreadsheet.getRange('A1').activate();
  var criteria = SpreadsheetApp.newFilterCriteria()
  .build();
  spreadsheet.getActiveSheet().getFilter().setColumnFilterCriteria(1, criteria);
  spreadsheet.getRange('C:C').activate();
  spreadsheet.getActiveSheet().insertColumnsBefore(spreadsheet.getActiveRange().getColumn(), 1);
  spreadsheet.getActiveRange().offset(0, 0, spreadsheet.getActiveRange().getNumRows(), 1).activate();
  spreadsheet.getRange('C1').activate();
  spreadsheet.getCurrentCell().setValue('abs_value');
  spreadsheet.getRange('E1').activate();
  spreadsheet.getRange('C1').copyTo(spreadsheet.getActiveRange(), SpreadsheetApp.CopyPasteType.PASTE_NORMAL, false);
  spreadsheet.getRange('C2').activate();
  spreadsheet.getCurrentCell().setFormula('=abs(B2)');
  spreadsheet.getActiveRange().autoFill(spreadsheet.getRange('C2:C243'), SpreadsheetApp.AutoFillSeries.DEFAULT_SERIES);
  spreadsheet.getRange('E2').activate();
  spreadsheet.getRange('C2').copyTo(spreadsheet.getActiveRange(), SpreadsheetApp.CopyPasteType.PASTE_NORMAL, false);
  spreadsheet.getRange('C2').activate();
  spreadsheet.getCurrentCell().getNextDataCell(SpreadsheetApp.Direction.DOWN).activate();
  spreadsheet.getRange('E243').activate();
  currentCell = spreadsheet.getCurrentCell();
  spreadsheet.getSelection().getNextDataRange(SpreadsheetApp.Direction.UP).activate();
  currentCell.activateAsCurrentCell();
  spreadsheet.getRange('C2').copyTo(spreadsheet.getActiveRange(), SpreadsheetApp.CopyPasteType.PASTE_NORMAL, false);
  spreadsheet.getRange('C243').activate();
  spreadsheet.getCurrentCell().getNextDataCell(SpreadsheetApp.Direction.UP).activate();
  spreadsheet.getActiveSheet().getFilter().sort(3, false);
  spreadsheet.getRange('C:C').activate();
  spreadsheet.getActiveSheet().hideColumns(spreadsheet.getActiveRange().getColumn(), spreadsheet.getActiveRange().getNumColumns());
  spreadsheet.getRange('D:D').activate();
  spreadsheet.getRange('E:E').activate();
  spreadsheet.getActiveSheet().hideColumns(spreadsheet.getActiveRange().getColumn(), spreadsheet.getActiveRange().getNumColumns());
  spreadsheet.getRange('F:F').activate();
  spreadsheet.getRange('A:D').activate();
  spreadsheet.getActiveSheet().autoResizeColumns(1, 4);
  spreadsheet.getRange('F5').activate();
  spreadsheet.rename('feature_importance');
};