const { app, BrowserWindow, dialog } = require('electron');
const { spawn } = require('child_process');
const http = require('http');
const path = require('path');

const WEB_HOST = '127.0.0.1';
const WEB_PORT = 5000;
const WEB_URL = `http://${WEB_HOST}:${WEB_PORT}`;

let pyProc = null;

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
  const repoRoot = path.resolve(__dirname, '..');
  const appPy = path.join(repoRoot, 'app.py');

  const pythonExe = process.env.PYTHON || 'python';

  const args = [
    appPy,
    '--no-serial',
    '--web-host',
    WEB_HOST,
    '--web-port',
    String(WEB_PORT),
  ];

  pyProc = spawn(pythonExe, args, {
    cwd: repoRoot,
    stdio: 'inherit',
    windowsHide: true,
  });

  pyProc.on('exit', (code, signal) => {
    pyProc = null;
    if (!app.isQuiting) {
      dialog.showErrorBox(
        'MWD Monitor',
        `El servidor Python terminó (code=${code}, signal=${signal}).`
      );
      app.quit();
    }
  });
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
