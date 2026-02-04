# MWD Monitor (WITSML)

Lectura de datos WITSML por puerto serial y emisión por WebSocket.

## Instalación

```bash
py -m pip install -r requirements.txt
```

## Generar ejecutable (Windows)

Para generar un `.exe` se requiere Python instalado en la máquina que compila. El ejecutable resultante se genera en `dist\mwdmonitor.exe`.

En PowerShell, desde `C:\edusurf\mwdmonitor`:

```powershell
./build.ps1
```

## Uso

Listar puertos disponibles:

```bash
py app.py --list-ports
```

También podés usar el lanzador por batch (recomendado en Windows):

```bat
run_mwdmonitor.bat --list-ports
```

Ejecutar en consola y elegir el puerto (interactivo):

```bat
run_mwdmonitor.bat
```

Ejecutar con lectura de puerto y servidor web:

```bash
py app.py --serial-port COM3 --baudrate 9600 --bytesize 8 --parity N --stopbits 1 --timeout 1.0
```

Con `.bat`:

```bat
run_mwdmonitor.bat --serial-port COM3 --baudrate 9600 --no-web
```

Solo consola (sin servidor web):

```bash
py app.py --serial-port COM3 --no-web
```

Los datos se imprimen en consola y, si el servidor web está activo, se emiten por WebSocket en el evento `witsml_data`.
