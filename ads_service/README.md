# Ads Service

This service handles integration with Google Ads and Facebook Ads APIs to retrieve ad metrics data.

## Secure Deployment

This service uses environment variables to securely access the Google Ads and Facebook Ads APIs. The credentials
are generated at runtime from these environment variables, which means no sensitive information is stored in the code repository.

### Required Environment Variables

To run the service, you need to set the following environment variables:

#### Google Ads API Credentials:

```
# Google Service Account details
GOOGLE_PROJECT_ID=your-project-id
GOOGLE_PRIVATE_KEY_ID=your-private-key-id
GOOGLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_CONTENT\n-----END PRIVATE KEY-----\n"
GOOGLE_CLIENT_EMAIL=your-service-account@your-project.iam.gserviceaccount.com
GOOGLE_CLIENT_ID=your-client-id
GOOGLE_CLIENT_X509_CERT_URL=https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com

# Google Ads API specific credentials
GOOGLE_ADS_DEVELOPER_TOKEN=your-developer-token
GOOGLE_ADS_IMPERSONATED_EMAIL=email-of-user-with-access@example.com
GOOGLE_ADS_LOGIN_CUSTOMER_ID=1234567890
```

**Important**: For the `GOOGLE_PRIVATE_KEY` variable, make sure to include the entire key including the `BEGIN PRIVATE KEY` and `END PRIVATE KEY` parts, and replace newlines with `\n`.

#### Database Credentials:

```
DB_HOST=your-database-host
DB_PORT=5432
DB_NAME=your-database-name
DB_USER=your-database-user
DB_PASSWORD=your-database-password
```

### Local Development

For local development, you can create a `.env` file in the project root and store these variables there.
**Never commit this file to the repository!**

### Production Deployment

For production deployment, make sure to:

1. Add these environment variables to your deployment environment (not in Git)
2. Pass them to the Docker container when running `docker-compose up`

Example:
```bash
# Create an .env file on your production server (not tracked in Git)
nano .env

# Add all the required environment variables
# Then deploy using that file
docker-compose --env-file .env up -d
```

### Security Best Practices

1. **Regularly rotate** the credentials and update the environment variables
2. **Restrict file permissions** on your .env file: `chmod 600 .env`
3. **Use a secrets management system** for production deployments

## API Endpoints

- `GET /health` - Health check endpoint
- `POST /retrieve_ad_metrics` - Trigger ad metrics retrieval (runs in background)
- `GET /test_google_ads` - Test Google Ads API connection 