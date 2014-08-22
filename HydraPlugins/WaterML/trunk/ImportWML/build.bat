pyinstaller --upx-dir=../../../upx ImportCSV.spec
copy dist\ImportCSV\* ..\CSVPlugin\plugins\ImportCSV\
copy plugin.xml ..\CSVPlugin\plugins\ImportCSV\plugin.xml
PAUSE