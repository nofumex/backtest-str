module.exports = {
  apps: [{
    name: "backtest-str",
    script: "scripts/python.mjs",
    args: "-m smartwallet.web",
    interpreter: "node",
    cwd: __dirname,
    env: { NODE_ENV: "production", PORT: "8000" },
    autorestart: true,
    kill_timeout: 15000,
    max_restarts: 10
  }]
};
