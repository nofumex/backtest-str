import { defineConfig } from '@playwright/test';
export default defineConfig({testDir:'./tests/ui', use:{baseURL:'http://127.0.0.1:5179', channel:process.platform==='win32'?'msedge':undefined},webServer:{command:'npm exec vite -- --host 127.0.0.1 --port 5179',url:'http://127.0.0.1:5179',reuseExistingServer:false}});
