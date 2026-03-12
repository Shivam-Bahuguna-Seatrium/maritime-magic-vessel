# 🚀 Quick Deploy Checklist

## Step 1: Deploy Backend (Python API)

### On Vercel Dashboard:
1. Click "Add New..." → "Project"
2. Select GitHub repo
3. **Root Directory**: Keep as root (default)
4. Click "Deploy" (vercel.json will auto-configure from `api/vercel.json`)
5. Wait for build to complete
6. **Copy backend URL** (e.g., `https://maritime-backend.vercel.app`)

### Environment Variables (Vercel Project Settings):
(Optional - only if you have Neo4j or OpenAI)
```
NEO4J_URI = neo4j://...
NEO4J_USERNAME = 
NEO4J_PASSWORD = 
OPENAI_API_KEY = sk-...
```

---

## Step 2: Deploy Frontend (React App)

### On Vercel Dashboard:
1. Click "Add New..." → "Project"
2. Select same GitHub repo
3. **Root Directory**: `maritime_vessel_system/frontend`
4. Click "Deploy" (vercel.json will auto-configure)
5. Wait for build to complete

### Environment Variables (Vercel Project Settings):
1. Go to Project Settings → Environment Variables
2. Add:
   ```
   VITE_API_URL = https://maritime-backend.vercel.app
   ```
   (Replace with your actual backend URL from Step 1)
3. **Important**: Set to **Production** environment
4. Click "Save"
5. Go to "Deployments" → Redeploy latest commit

---

## Step 3: Test

1. Visit frontend URL
2. Open Developer Console (F12)
3. Check that API calls are going to your backend URL
4. Upload CSV file and test features

---

## URLs You'll Have

After both deployments:

| Service | URL | Environment|
|---------|-----|--------------|
| **Backend API** | `https://maritime-backend.vercel.app` | n/a |
| **Frontend App** | `https://maritime-frontend.vercel.app` | `VITE_API_URL=https://maritime-backend.vercel.app` |

---

## Troubleshooting

**Backend not found?**
- Make sure frontend has correct `VITE_API_URL` set
- Check browser console (F12) to see actual URL being called
- Verify no trailing slash on backend URL

**Still getting 500 errors?**
- Go to Vercel → Your Backend Project → "Function Logs"
- Look for error messages from Python
- Check if Neo4j/OpenAI credentials needed

---

## Local Testing (Before Deploy)

```bash
# Terminal 1: Backend
cd api
pip install -r requirements.txt
uvicorn index:app --reload --port 8000

# Terminal 2: Frontend
cd maritime_vessel_system/frontend
echo "VITE_API_URL=http://localhost:8000" > .env.local
npm install
npm run dev
```

Visit: `http://localhost:5173`

---

**That's it! Two services, one repo, clean & scalable. 🎉**
