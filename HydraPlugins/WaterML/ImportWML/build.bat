pyinstaller --upx-dir=../../../upx ImportWML.spec
copy dist\ImportWML\* ..\WaterMLPlugin\plugins\ImportWML\
copy plugin.xml ..\WaterMLPlugin\plugins\ImportWML\plugin.xml
rd /S /Q build
PAUSE