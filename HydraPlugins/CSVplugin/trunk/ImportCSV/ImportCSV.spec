# -*- mode: python -*-
a = Analysis(['ImportCSV.py'],
             pathex=['C:\\Users\\steve\\Documents\\svn\\HYDRA\\HydraPlugins\\CSVplugin\\trunk\\ImportCSV'],
             hiddenimports=[],
             hookspath=None,
             runtime_hooks=None)
pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='ImportCSV.exe',
          debug=False,
          strip=None,
          upx=True,
          console=True )
#a.binaries = [x for x in a.binaries if x[0].find('linalg') < 0]
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=None,
               upx=True,
               name='ImportCSV')
