pyinstaller --upx-dir=../../../upx ImportWML.spec -y
xcopy dist\ImportWML\* ..\WaterMLPlugin\plugins\ImportWML\ /Y /E
copy plugin.xml ..\WaterMLPlugin\plugins\ImportWML\
copy wml16.png ..\WaterMLPlugin\plugins\ImportWML\
copy wml32.png ..\WaterMLPlugin\plugins\ImportWML\
copy WaterML_Timeseries.pdf ..\WaterMLPlugin\plugins\ImportWML\
xcopy SampleData ..\WaterMLPlugin\plugins\ImportWML\SampleData /Y /E
PAUSE