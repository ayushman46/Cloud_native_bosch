import {defineConfig} from 'vite';
import react from '@vitejs/plugin-react';
// `npm run dev:demo` points the dashboard at a mock backend on 8010 so it never touches the real stack on 8000
export default defineConfig(({mode}) => {
  const api = process.env.CHAOSAI_API_URL || (mode === 'demo' ? 'http://127.0.0.1:8010' : 'http://127.0.0.1:8000');
  return {plugins: [react()], server: {host: '127.0.0.1', proxy: {'/api': api, '/healthz': api}}};
});
