# -*- mode: python -*-
a = Analysis(['ExportCSV.py'],
             pathex=['C:\\Users\\steve\\Documents\\svn\\HYDRA\\HydraPlugins\\CSVplugin\\trunk\\ImportCSV'],
             hiddenimports=[],
             hookspath=None,
             runtime_hooks=None)
pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='ExportCSV.exe',
          debug=False,
          strip=None,
          upx=True,
          console=True )
