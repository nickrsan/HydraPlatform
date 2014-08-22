pyinstaller --upx-dir=../../../upx ExportCSV.spec
copy dist\ExportCSV\* ..\CSVPlugin\plugins\ExportCSV\
copy plugin.xml ..\CSVPlugin\plugins\ExportCSV\plugin.xml
PAUSE