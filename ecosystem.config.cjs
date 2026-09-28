module.exports = {
  apps: [{
    name: "backtest-str",
    script: "scripts/python.mjs",
    args: "-m smartwallet.web",
    interpreter: "node",
    cwd: __dirname,
    env: {
      NODE_ENV: "production",
      PORT: "8000",
      SMARTWALLET_DB: require("path").join(__dirname, "data", "smartwallet.db")
    },
    autorestart: true,
    kill_timeout: 15000,
    max_restarts: 10
  }]
};
