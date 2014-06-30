pyinstaller --upx-dir=../../../upx ExportCSV.spec
copy dist\ExportCSV.exe ..\CSVPlugin\plugins\ExportCSV\ExportCSV.exe
copy plugin.xml ..\CSVPlugin\plugins\ExportCSV\plugin.xml
PAUSE