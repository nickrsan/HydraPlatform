pyinstaller -F --upx-dir=..\..\..\upx ImportCSV.py
copy dist\ImportCSV.exe ..\CSVPlugin\plugins\ImportCSV\ImportCSV.exe
copy plugin.xml ..\CSVPlugin\plugins\ImportCSV\plugin.xml
PAUSE