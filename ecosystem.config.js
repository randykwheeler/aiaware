module.exports = {
  apps: [{
    name: 'aiaware',
    script: '/root/aiaware/start.sh',
    cwd: '/root/aiaware',
    env: {
      AIAWARE_MODE: 'reader',
      AIAWARE_PORT: '10073',
      AIAWARE_DIR: '/root/aiaware',
      PYTHONUNBUFFERED: '1'
    },
    restart_delay: 3000,
    max_restarts: 10
  }]
};
