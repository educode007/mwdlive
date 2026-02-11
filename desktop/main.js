const { app, BrowserWindow, dialog, Menu } = require('electron');
const { spawn } = require('child_process');
const http = require('http');
const fs = require('fs');
const path = require('path');

const WEB_HOST = '127.0.0.1';
const WEB_PORT = 5000;
const WEB_URL = `http://${WEB_HOST}:${WEB_PORT}`;

let pyProc = null;

function ensureDirSync(dirPath) {
  try {
    fs.mkdirSync(dirPath, { recursive: true });
  } catch (_) {
  }
}

function safeReadLogTail(logPath, maxBytes = 8000) {
  try {
    const stat = fs.statSync(logPath);
    const start = Math.max(0, stat.size - maxBytes);
    const fd = fs.openSync(logPath, 'r');
    try {
      const buf = Buffer.alloc(stat.size - start);
      fs.readSync(fd, buf, 0, buf.length, start);
      return buf.toString('utf8');
    } finally {
      fs.closeSync(fd);
    }
  } catch (_) {
    return '';
  }
}

function waitForHttp(url, timeoutMs = 15000, intervalMs = 250) {
  const deadline = Date.now() + timeoutMs;

  return new Promise((resolve, reject) => {
    const attempt = () => {
      const req = http.get(url, (res) => {
        res.resume();
        resolve();
      });

      req.on('error', () => {
        if (Date.now() > deadline) {
          reject(new Error(`Timed out waiting for ${url}`));
          return;
        }
        setTimeout(attempt, intervalMs);
      });

      req.setTimeout(2000, () => {
        req.destroy();
      });
    };

    attempt();
  });
}

function startPythonServer() {
  const isPackaged = app.isPackaged;
  const backendDir = isPackaged
    ? path.join(process.resourcesPath, 'backend')
    : path.resolve(__dirname, '..');
  const appPy = isPackaged
    ? path.join(backendDir, 'app.py')
    : path.join(backendDir, 'app.py');

  const serialPort = process.env.DESKTOP_SERIAL_PORT || process.env.SERIAL_PORT || '';
  const baudrateRaw = process.env.DESKTOP_BAUDRATE || process.env.BAUDRATE || '';
  const baudrate = Number(baudrateRaw) || 9600;

  const logDir = app.getPath('userData');
  ensureDirSync(logDir);
  const logPath = path.join(logDir, 'py-backend.log');
  let logStream = null;
  try {
    logStream = fs.createWriteStream(logPath, { flags: 'a' });
    logStream.write(`\n=== Backend start ${new Date().toISOString()} ===\n`);
    logStream.write(`packaged=${String(isPackaged)}\n`);
    logStream.write(`backendDir=${backendDir}\n`);
    logStream.write(`appPy=${appPy}\n`);
  } catch (_) {
    logStream = null;
  }

  const envPython = process.env.PYTHON;
  const pythonCandidates = [];
  if (envPython) pythonCandidates.push({ exe: envPython, prefixArgs: [] });
  // Prefer Windows Python launcher if available
  pythonCandidates.push({ exe: 'py', prefixArgs: ['-3'] });
  pythonCandidates.push({ exe: 'python', prefixArgs: [] });
  pythonCandidates.push({ exe: 'python3', prefixArgs: [] });

  const attachHandlers = (proc) => {
    if (proc.stdout) {
      proc.stdout.on('data', (d) => {
        try {
          if (logStream) logStream.write(d);
        } catch (_) {
        }
      });
    }
    if (proc.stderr) {
      proc.stderr.on('data', (d) => {
        try {
          if (logStream) logStream.write(d);
        } catch (_) {
        }
      });
    }

    proc.on('exit', (code, signal) => {
      if (pyProc === proc) pyProc = null;
      try {
        if (logStream) {
          logStream.write(`\n=== Backend exit code=${code} signal=${signal} ${new Date().toISOString()} ===\n`);
          logStream.end();
        }
      } catch (_) {
      }
      if (!app.isQuiting) {
        const tail = safeReadLogTail(logPath);
        dialog.showErrorBox(
          'MWD Monitor',
          `El servidor Python terminó (code=${code}, signal=${signal}).\n\nLog: ${logPath}\n\nÚltimas líneas:\n${tail}`
        );
        app.quit();
      }
    });
  };

  const spawnAttempt = (idx) => {
    const cand = pythonCandidates[idx];
    if (!cand) {
      const tail = safeReadLogTail(logPath);
      dialog.showErrorBox(
        'MWD Monitor',
        `No se pudo iniciar Python (probé: ${pythonCandidates.map((c) => c.exe).join(', ')}).\n\nSugerencia: instalá Python 3.11+ o definí la variable de entorno PYTHON con la ruta a python.exe.\n\nLog: ${logPath}\n\nÚltimas líneas:\n${tail}`
      );
      app.quit();
      return;
    }

    const args = [
      ...cand.prefixArgs,
      appPy,
      '--no-serial',
      '--web-host',
      WEB_HOST,
      '--web-port',
      String(WEB_PORT),
    ];

    if (serialPort) {
      const noSerialIdx = args.indexOf('--no-serial');
      if (noSerialIdx >= 0) args.splice(noSerialIdx, 1);
      args.push('--serial-port', String(serialPort));
      args.push('--baudrate', String(baudrate));
    }

    try {
      if (logStream) {
        logStream.write(`pythonExe=${cand.exe}\n`);
        logStream.write(`pythonArgs=${JSON.stringify(args)}\n`);
        logStream.write(`serialPort=${String(serialPort)} baudrate=${String(baudrate)}\n`);
      }
    } catch (_) {
    }

    const childEnv = { ...process.env, MWDMONITOR_USERDATA: logDir };

    pyProc = spawn(cand.exe, args, {
      cwd: backendDir,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
      env: childEnv,
    });

    attachHandlers(pyProc);

    pyProc.on('error', (err) => {
      const msg = String(err && err.message ? err.message : err);
      const isNotFound = msg.includes('ENOENT') || msg.includes('not found');
      try {
        if (logStream) {
          logStream.write(`\n=== Backend spawn error (${cand.exe}) ${new Date().toISOString()} ===\n`);
          logStream.write(String(err && err.stack ? err.stack : err));
          logStream.write('\n');
        }
      } catch (_) {
      }

      if (isNotFound) {
        spawnAttempt(idx + 1);
        return;
      }

      if (!app.isQuiting) {
        const tail = safeReadLogTail(logPath);
        dialog.showErrorBox(
          'MWD Monitor',
          `No se pudo iniciar Python (${cand.exe}).\n\nSugerencia: instalá Python 3.11+ o definí la variable de entorno PYTHON con la ruta a python.exe.\n\nLog: ${logPath}\n\nÚltimas líneas:\n${tail}`
        );
        app.quit();
      }
    });
  };

  spawnAttempt(0);
}

function stopPythonServer() {
  if (!pyProc) return;
  try {
    pyProc.kill();
  } catch (_) {
  }
  pyProc = null;
}

async function createWindow() {
  try {
    const template = [
      {
        label: 'Help',
        submenu: [
          {
            label: 'About',
            click: async () => {
              try {
                await dialog.showMessageBox({
                  type: 'info',
                  title: 'About',
                  message: 'MWD Monitor Desktop',
                  detail: `Version: ${app.getVersion()}`,
                  buttons: ['OK'],
                });
              } catch (_) {
              }
            },
          },
        ],
      },
    ];
    const menu = Menu.buildFromTemplate(template);
    Menu.setApplicationMenu(menu);
  } catch (_) {
  }

  const win = new BrowserWindow({
    width: 1280,
    height: 800,
    webPreferences: {
      contextIsolation: true,
    },
  });

  await win.loadURL(WEB_URL);
}

app.on('before-quit', () => {
  app.isQuiting = true;
  stopPythonServer();
});

app.whenReady().then(async () => {
  try {
    startPythonServer();
    await waitForHttp(WEB_URL);
    await createWindow();
  } catch (e) {
    dialog.showErrorBox('MWD Monitor', String(e && e.message ? e.message : e));
    app.quit();
  }
});

app.on('window-all-closed', () => {
  app.quit();
});
