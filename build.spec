# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec 文件
# 注意：这个文件用于打包 http_stress.py
# 要打包 mqtt_client.py，请使用 build_mqtt.spec 或直接使用命令行

block_cipher = None

a = Analysis(
    ['http_stress.py'],
    pathex=[],
    binaries=[],
    datas=[],  # 如果需要包含 devices.csv，可以添加: [('devices.csv', '.')]
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='http_stress',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

