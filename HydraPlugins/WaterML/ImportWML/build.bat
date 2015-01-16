pyinstaller --upx-dir=../../../upx ImportWML.spec
xcopy dist\ImportWML\* ..\WaterMLPlugin\plugins\ImportWML\ /Y /s
copy plugin.xml ..\WaterMLPlugin\plugins\ImportWML\plugin.xml
rd /S /Q build
PAUSE