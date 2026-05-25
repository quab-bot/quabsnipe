// ecosystem.config.js — PM2 process manager config
// Start with: pm2 start ecosystem.config.js
// Auto-restart: pm2 startup && pm2 save

module.exports = {
  apps: [
    {
      name:        "solana-sniper",
      script:      "main.py",
      interpreter: "python3",
      watch:       false,
      autorestart: true,
      restart_delay: 5000,    // Wait 5s before restarting after crash
      max_restarts:  50,
      env: {
        PYTHONUNBUFFERED: "1",
      },
      error_file:  "logs/pm2_error.log",
      out_file:    "logs/pm2_out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss",
    },
  ],
};
