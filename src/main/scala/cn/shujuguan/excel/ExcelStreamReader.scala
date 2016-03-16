package cn.shujuguan.excel

import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.util.SAXHelper
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler
import org.apache.poi.xssf.eventusermodel.{ReadOnlySharedStringsTable, XSSFReader, XSSFSheetXMLHandler}
import org.apache.poi.xssf.usermodel.{XSSFComment, XSSFSheet, XSSFWorkbook}
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTSheet
import org.xml.sax.InputSource

/**
  * Created by yueyang on 3/16/16.
  */
object ExcelStreamReader {
  def main(args: Array[String]) {
    val workbook = new XSSFWorkbook(this.getClass.getResourceAsStream("/test.xlsx")) {
      override def parseSheet(shIdMap: java.util.Map[String, XSSFSheet], ctSheet: CTSheet): Unit = {
        println("你好")
      }
    }

    val opcPackage = workbook.getPackage

    val xssfReader = new XSSFReader(opcPackage)

    val iter: XSSFReader.SheetIterator = xssfReader.getSheetsData.asInstanceOf[XSSFReader.SheetIterator]

    while (iter.hasNext) {
      val sheetInputStream = iter.next()
      println(iter.getSheetName)

      val sheetInputSource = new InputSource(sheetInputStream)

      val xmlReader = SAXHelper.newXMLReader()

      val table = new ReadOnlySharedStringsTable(opcPackage)

      val sheetXMLHandler = new XSSFSheetXMLHandler(
        xssfReader.getStylesTable,
        null,
        table,
        new StreamSheetContentsHandler(),
        new DataFormatter(),
        false)

      xmlReader.setContentHandler(sheetXMLHandler)
      xmlReader.parse(sheetInputSource)

      sheetInputStream.close()
    }

    workbook.close()
  }

  class StreamSheetContentsHandler extends SheetContentsHandler {
    override def endRow(rowNum: Int): Unit = {
      println(rowNum)
    }

    override def startRow(rowNum: Int): Unit = {
      println(rowNum)
    }

    override def cell(cellReference: String, formattedValue: String, comment: XSSFComment): Unit = {
      println(formattedValue)
    }

    override def headerFooter(text: String, isHeader: Boolean, tagName: String): Unit = {
      println(text)
    }
  }
}
