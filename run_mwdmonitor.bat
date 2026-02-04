@echo off
setlocal

REM Resolve project directory (folder of this .bat)
set "PROJDIR=%~dp0"

if "%~1"=="" goto :default_run

call :find_python
if errorlevel 1 goto :no_python

call :check_deps
if errorlevel 1 goto :no_deps

echo Ejecutando:
echo   "%PYEXE%" "%PROJDIR%app.py" %*
echo.

call "%PYEXE%" "%PROJDIR%app.py" %*
set "EXITCODE=%errorlevel%"
if not "%EXITCODE%"=="0" (
  echo.
  echo La aplicacion termino con errorlevel=%EXITCODE%
  pause
)

endlocal
exit /b %EXITCODE%

:usage
echo Uso:
echo   run_mwdmonitor.bat --list-ports
echo   run_mwdmonitor.bat ^(inicia web local y pide elegir puerto^)
echo   run_mwdmonitor.bat --serial-port COM3 --baudrate 9600
echo   run_mwdmonitor.bat --serial-port COM3 --baudrate 9600 --no-web
echo.
echo Web UI:
echo   http://127.0.0.1:5000/
echo.
pause
endlocal
exit /b 0

:default_run
call :find_python
if errorlevel 1 goto :no_python

call :check_deps
if errorlevel 1 goto :no_deps

echo Ejecutando:
echo   "%PYEXE%" "%PROJDIR%app.py"
echo.

call "%PYEXE%" "%PROJDIR%app.py"
set "EXITCODE=%errorlevel%"
if not "%EXITCODE%"=="0" (
  echo.
  echo La aplicacion termino con errorlevel=%EXITCODE%
  pause
)

endlocal
exit /b %EXITCODE%

:no_python
echo Python no encontrado en PATH.
echo Instala Python 3.11+ y habilita PATH, o instala el Python Launcher (py).
echo.
pause
endlocal
exit /b 1

:no_deps
echo Dependencias no instaladas.
echo Ejecuta:
echo   %PYEXE% -m pip install -r "%PROJDIR%requirements.txt"
echo.
pause
endlocal
exit /b 1

:find_python
set "PYEXE="

if exist "%PROJDIR%.venv\Scripts\python.exe" (
  set "PYEXE=%PROJDIR%.venv\Scripts\python.exe"
  exit /b 0
)

where python >nul 2>&1
if not errorlevel 1 (
  set "PYEXE=python"
  exit /b 0
)

where py >nul 2>&1
if not errorlevel 1 (
  set "PYEXE=py"
  exit /b 0
)

exit /b 1

:check_deps
"%PYEXE%" -c "import serial, flask, flask_socketio" >nul 2>&1
if errorlevel 1 exit /b 1
exit /b 0
