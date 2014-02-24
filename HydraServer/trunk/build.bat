ECHO 'Starting build...'
pyinstaller --hidden-import=spyne.service -y server.py
mkdir dist\server\HydraLib
mkdir dist\server\HydraLib\static
copy ..\..\HydraLib\trunk\HydraLib\static\unit_definitions.xml dist\server\HydraLib\static
copy ..\..\HydraLib\trunk\HydraLib\static\user_units.xml dist\server\HydraLib\static
PAUSE