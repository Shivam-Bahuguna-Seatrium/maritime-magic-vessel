/**
 * Debug utility for console logging
 * Provides consistent, colored logging across the frontend
 */

const DEBUG = {
  PREFIX: '[MARITIME-DEBUG]',
  
  log: (section, message, data) => {
    const timestamp = new Date().toLocaleTimeString();
    console.log(`%c${DEBUG.PREFIX} [${section}] ${timestamp}`, 'color: #0066cc; font-weight: bold;', message, data || '');
  },
  
  info: (section, message, data) => {
    const timestamp = new Date().toLocaleTimeString();
    console.info(`%c${DEBUG.PREFIX} [${section}] ${timestamp}`, 'color: #00aa00; font-weight: bold;', message, data || '');
  },
  
  warn: (section, message, data) => {
    const timestamp = new Date().toLocaleTimeString();
    console.warn(`%c${DEBUG.PREFIX} [${section}] ${timestamp}`, 'color: #ff8800; font-weight: bold;', message, data || '');
  },
  
  error: (section, message, data) => {
    const timestamp = new Date().toLocaleTimeString();
    console.error(`%c${DEBUG.PREFIX} [${section}] ${timestamp}`, 'color: #ff0000; font-weight: bold;', message, data || '');
  },

  api: (method, url, data) => {
    DEBUG.log('API-REQUEST', `${method} ${url}`, data);
  },

  apiResponse: (method, url, status, data) => {
    const color = status >= 200 && status < 300 ? '#00aa00' : '#ff0000';
    const prefix = `%c${DEBUG.PREFIX} [API-RESPONSE] ${new Date().toLocaleTimeString()}`;
    console.log(prefix, `color: ${color}; font-weight: bold;`, `${status} ${method} ${url}`, data || '');
  },

  apiError: (method, url, error) => {
    DEBUG.error('API-ERROR', `${method} ${url}`, error);
  }
};

export default DEBUG;
