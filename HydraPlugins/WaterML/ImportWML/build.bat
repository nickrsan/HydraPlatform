pyinstaller --upx-dir=../../../upx ImportWML.spec -y
xcopy dist\ImportWML\* ..\WaterMLPlugin\plugins\ImportWML\ /Y /E
copy plugin.xml ..\WaterMLPlugin\plugins\ImportWML\plugin.xml
PAUSE