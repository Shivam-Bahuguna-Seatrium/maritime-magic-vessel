# Deployment Guide - Two Separate Services

This project is now split into **2 separate Vercel projects** for cleaner deployment:
- **Backend**: Python FastAPI REST API
- **Frontend**: React SPA

## Quick Setup

### 1. Deploy Backend First

```bash
# Navigate to backend
cd api

# Deploy to Vercel
vercel deploy --prod
```

You'll get a URL like: `https://maritime-backend-api.vercel.app`

### 2. Deploy Frontend

```bash
# Navigate to frontend
cd maritime_vessel_system/frontend

# Create .env with backend URL
echo "VITE_API_URL=https://maritime-backend-api.vercel.app" > .env.production.local

# Deploy to Vercel
vercel deploy --prod
```

---

## Detailed Setup Instructions

### Backend Deployment (Vercel)

**Prerequisites:**
- Vercel account: https://vercel.com
- Git repository connected to Vercel

**Steps:**

1. **Create new Vercel project for backend:**
   - Go to https://vercel.com/dashboard
   - Click "Add New..." > "Project"
   - Select your GitHub repository
   - Choose "Root Directory" and set to project root (NOT api folder)
   - Click "Deploy"

2. **Configure environment variables in Vercel:**
   - Go to Project Settings > Environment Variables
   - Add these (optional, for Neo4j or OpenAI):
     ```
     NEO4J_URI = neo4j://your-database-uri
     NEO4J_USERNAME = your-username
     NEO4J_PASSWORD = your-password
     OPENAI_API_KEY = sk-****
     ```
   - Save and redeploy

3. **Copy the backend URL:**
   - After deployment, copy your backend URL
   - Example: `https://maritime-backend-api.vercel.app`

### Frontend Deployment (Vercel)

**Steps:**

1. **Create new Vercel project for frontend:**
   - Go to https://vercel.com/dashboard
   - Click "Add New..." > "Project"
   - Select your GitHub repository
   - Set "Root Directory" to `maritime_vessel_system/frontend`
   - Click "Deploy"

2. **Configure environment variables:**
   - Go to Project Settings > Environment Variables
   - Add:
     ```
     VITE_API_URL = https://maritime-backend-api.vercel.app
     ```
     (Replace with your actual backend URL from step above)
   - Make sure it's set for **Production** environment
   - Save and redeploy

3. **Verify deployment:**
   - Visit your frontend URL
   - Check browser console (F12) for API calls
   - Should see successful API responses

---

## Local Development

### Run Backend (Python)

```bash
cd api
pip install -r requirements.txt
uvicorn index:app --reload --port 8000
```

Backend runs at: `http://localhost:8000`

### Run Frontend (Node.js)

```bash
cd maritime_vessel_system/frontend
npm install
npm run dev
```

Frontend runs at: `http://localhost:5173`

**Frontend .env for local dev:**
```bash
VITE_API_URL=http://localhost:8000
```

---

## Environment Variables Overview

### Backend (api/.env)

```bash
# Optional: Neo4j database
NEO4J_URI=neo4j://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=password

# Optional: OpenAI integration
OPENAI_API_KEY=sk-****
```

### Frontend (maritime_vessel_system/frontend/.env)

```bash
# Must point to backend API
VITE_API_URL=https://maritime-backend-api.vercel.app
```

---

## Troubleshooting

### Frontend not connecting to backend
- ✅ Verify `VITE_API_URL` is set correctly in Vercel
- ✅ Check browser console (F12) for actual API URL being called
- ✅ Ensure backend URL doesn't have trailing slash
- ✅ Check CORS is enabled on backend (it is by default)

### Backend API returning 500 errors
- ✅ Check "Function Logs" in Vercel dashboard
- ✅ Verify all dependencies installed: `pip list`
- ✅ Check if Neo4j/OpenAI credentials are valid
- ✅ Backend should fall back to in-memory graph if Neo4j unavailable

### CSS/JS files not loading
- ✅ Ensure frontend built successfully: `npm run build`
- ✅ Check `dist/` folder exists with assets
- ✅ Verify vercel.json has correct `outputDirectory`

---

## Separate Repositories (Optional)

For even cleaner separation, you can split into 2 repos:

**Option A: Single repo, 2 projects** (Current setup)
- Pros: Shared git history, single team
- Cons: Need to manage both projects in same repo

**Option B: Two separate repos** (Advanced)
```bash
# Backend repo
git clone backend-repo
vercel link

# Frontend repo  
git clone frontend-repo
vercel link
```

---

## Next Steps

1. ✅ Deploy backend first and note the URL
2. ✅ Deploy frontend with VITE_API_URL set
3. ✅ Test file upload and graph visualization
4. ✅ Monitor logs in Vercel dashboard
5. ✅ Share both URLs with your team

Good luck! 🚢
