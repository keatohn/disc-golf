# Disc Golf User App

An interface for managing UDisc user credentials with AWS Secrets Manager storage.

## Features

- 👥 **User Management** - Add, update, and delete users
- 🔐 **Secure Storage** - AWS Secrets Manager storage
- ✅ **Credential Validation** - Validates credentials before storing
- 🎨 **Responsive Design** - Works on desktop and mobile
- 🚀 **Easy Deployment** - Heroku deployment

## Setup

### 1. Install Dependencies

```bash
cd disc-golf-user-app
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create a `.env` file in the project root:

```bash
AWS_SECRETS_MANAGER_ACCESS_KEY_ID=your_access_key
AWS_SECRETS_MANAGER_SECRET_ACCESS_KEY=your_secret_key
AWS_SECRETS_MANAGER_NAME=secrets_manager_name
AWS_DEFAULT_REGION=your-region
FLASK_SECRET_KEY=your-secure-secret-key
FLASK_ENV=development
```

### 3. Run the Web Interface

```bash
# Option 1: Use the startup script
python run.py

# Option 2: Use Flask CLI
export FLASK_APP=app.py
export FLASK_ENV=development
python -m flask run --host=0.0.0.0 --port=5001
```

The web interface will be available at `http://localhost:5001`

## Usage

### Adding Users

1. Open the web interface in your browser
2. Enter a UDisc username and password
3. Click "Add User"
4. **The app will validate credentials by attempting login before storage**

### Updating Passwords

1. Enter the username and new password
2. Click "Update Password"
3. **The app will validate the new credentials by attempting login before storage**

### Deleting Users

1. Enter the username and password
2. Click "Delete User"
3. **Requires correct credentials for deletion**

### Viewing Users

- All existing users are displayed with creation dates
- Users are stored securely in AWS Secrets Manager

## Deployment

### Heroku Deployment

The app is configured for easy Heroku deployment:

```bash
# Create Heroku app
heroku create disc-golf-user

# Set environment variables
heroku config:set AWS_SECRETS_MANAGER_ACCESS_KEY_ID=your_key --app disc-golf-user
heroku config:set AWS_SECRETS_MANAGER_SECRET_ACCESS_KEY=your_secret --app disc-golf-user
heroku config:set AWS_SECRETS_MANAGER_NAME=secrets-manager-name --app disc-golf-user
heroku config:set AWS_DEFAULT_REGION=your-region --app disc-golf-user
heroku config:set FLASK_SECRET_KEY=your-secret-key --app disc-golf-user
heroku config:set FLASK_ENV=production --app disc-golf-user

# Deploy
git add .
git commit -m "Initial commit"
git push heroku main
```

Your app will be available at `https://disc-golf-user-xxxxx.herokuapp.com`

## File Structure

```
disc-golf-user-app/
├── app.py                          # Main Flask application
├── validate_user_credentials.py    # UDisc API validation
├── run.py                          # Startup script
├── templates/                      # HTML templates
│   └── index.html                 # Main page template
├── requirements.txt                # Python dependencies
├── .gitignore                     # Git ignore rules
└── README.md                      # This file
```