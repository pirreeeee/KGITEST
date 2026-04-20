@echo off
setlocal
cd /d "%~dp0"
if not exist ".tmp" mkdir ".tmp"
set "UV_CACHE_DIR=%CD%\.uv-cache"
set "UV_PYTHON_INSTALL_DIR=%CD%\.uv-python"
set "TEMP=%CD%\.tmp"
set "TMP=%CD%\.tmp"

set "UV_EXE=%USERPROFILE%\.local\bin\uv.exe"
if exist "%UV_EXE%" (
  "%UV_EXE%" run --python 3.12 python -m unittest discover -s tests
) else (
  where uv >nul 2>nul
  if %ERRORLEVEL% EQU 0 (
    uv run --python 3.12 python -m unittest discover -s tests
  ) else (
    python -m unittest discover -s tests
  )
)
