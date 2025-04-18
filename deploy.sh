   #!/bin/bash
set -e

   # Pull latest code without overwriting .env
git fetch origin main
reset --hard origin/main
   
   # Generate credential files from environment variables
cd ads_service
python -c "from main import setup_google_ads_credentials; setup_google_ads_credentials()"
cd ..

   # Build and restart containers
docker compose down
docker compose build --no-cache
docker compose up -d