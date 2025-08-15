#!/usr/bin/env python3
"""
Startup script for the Disc Golf User Manager web interface.
"""

from app import app
import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import from the main project
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import and run the Flask app

if __name__ == '__main__':
    # Set default environment variables if not already set
    if not os.getenv('FLASK_ENV'):
        os.environ['FLASK_ENV'] = 'development'

    if not os.getenv('FLASK_SECRET_KEY'):
        print("Warning: FLASK_SECRET_KEY not set. Please set this environment variable.")
        sys.exit(1)

    # Get port from environment or default to 5001
    port = int(os.getenv('PORT', 5001))

    print(f"🚀 Starting Disc Golf User Manager...")
    print(f"🌐 Web interface will be available at: http://localhost:{port}")
    print(
        f"📁 Using AWS Secrets Manager: {os.getenv('AWS_SECRETS_MANAGER_NAME', 'udisc-users')}")
    print(f"🔑 Make sure your AWS credentials are configured!")
    print(f"⏹️  Press Ctrl+C to stop the server")
    print("-" * 60)

    try:
        app.run(host='0.0.0.0', port=port, debug=True)
    except KeyboardInterrupt:
        print("\n👋 Shutting down Disc Golf User Manager...")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)
