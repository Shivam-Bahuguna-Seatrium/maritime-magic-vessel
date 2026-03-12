/**
 * API Configuration - Use VITE_API_URL environment variable
 * 
 * In development: VITE_API_URL="http://localhost:8000"
 * In production: Set via Vercel environment variables
 */

export const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const apiCall = async (method, endpoint, data = null) => {
  const url = `${API_BASE_URL}${endpoint}`;
  
  const options = {
    method,
    headers: {
      'Content-Type': 'application/json',
    },
  };
  
  if (data) {
    options.body = JSON.stringify(data);
  }
  
  const response = await fetch(url, options);
  
  if (!response.ok) {
    throw new Error(`API Error: ${response.status} ${response.statusText}`);
  }
  
  return response.json();
};

export const apiFormData = async (method, endpoint, formData) => {
  const url = `${API_BASE_URL}${endpoint}`;
  
  const options = {
    method,
    body: formData,
  };
  
  const response = await fetch(url, options);
  
  if (!response.ok) {
    throw new Error(`API Error: ${response.status} ${response.statusText}`);
  }
  
  return response.json();
};
