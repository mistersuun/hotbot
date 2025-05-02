# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['salesforce_scraper_gui.py'],
    pathex=[],
    binaries=[],
    datas=[('config.json', '.'), ('data', 'data'), ('helpers', 'helpers'), ('chrome', 'chrome')],
    hiddenimports=['customtkinter', 'undetected_chromedriver'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='salesforce_scraper_gui',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
