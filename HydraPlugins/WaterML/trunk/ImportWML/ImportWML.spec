# -*- mode: python -*-
a = Analysis(['ImportWML.py'],
             pathex=['C:\\Users\\steve\\Documents\\svn\\HYDRA\\HydraPlugins\\WMLplugin\\trunk\\ImportWML'],
             hiddenimports=[],
             hookspath=None,
             runtime_hooks=None)
pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='ImportWML.exe',
          debug=False,
          strip=None,
          upx=True,
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=None,
               upx=True,
               name='ImportWML')
