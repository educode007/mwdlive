$ErrorActionPreference = "Stop"

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "Python no encontrado. Instala Python 3.11+ y asegurate de que 'python' esté en PATH." -ForegroundColor Red
    exit 1
}

python -m venv .venv

$venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"

& $venvPython -m pip install --upgrade pip
& $venvPython -m pip install -r requirements-build.txt

& $venvPython -m PyInstaller --noconfirm --clean --onefile --name mwdmonitor app.py

Write-Host "Build OK. Ejecutable generado en: dist\mwdmonitor.exe" -ForegroundColor Green
