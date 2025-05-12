import os
import yaml
import subprocess
import getpass

# ----------------------------------------
# 1. Creating the .env.example file
# ----------------------------------------

# Create a .env file for the developer, .env.example for an example to put on GitHub, and another for production
# Only .env.example will not be gitignored as it is safe for GitHub
env_example_path = ".env.example"
default_env_content = """POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=misinformation_databse
"""

# Write the content into the .env file
with open(env_example_path, "w") as f:
    f.write(default_env_content)

print("The .env.example file was created and is safe to commit to GitHub")


# ----------------------------------------
# 2. Creating the .env file
# ----------------------------------------

# The .env file will be created, if it does not yet exist, using manually inputted information from the user
env_path = ".env"

if not os.path.exists(env_path): # If not found in the directory
    print("Creating a new .env file that will not be pushed to GitHub.\n")

    user = input("POSTGRES_USER: ").strip() # Strip to remove any leading or trailing white spaces
    pw = getpass.getpass("POSTGRES_PASSWORD: ").strip()
    db = input("POSTGRES_DB: ").strip()

    # Safely store this data in the .env file
    with open(env_path, "w") as f:
        f.write(f"POSTGRES_USER={user}\n")
        f.write(f"POSTGRES_PASSWORD={pw}\n")
        f.write(f"POSTGRES_DB={db}\n")

    print("The .env file created for developper use")
else:
    print("A .env file already exists")


# ----------------------------------------
# 3. Creating the docker-composer.yml file
# ----------------------------------------

# First check if this is currently being used for development or production
dev_mode = input("Is this for development? (y/n): ").lower() == "y"

# Creating the docker-compose.yml file
postgres_service = {
    'image': 'cgr.dev/chainguard/postgres:latest', # Use the official chainguard secure PostgreSQL image, latest version
    'container_name': 'misinfo-postgres', # Name the container for easier management
    'environment': {
        'POSTGRES_USER': '${POSTGRES_USER}', # Username for the DB from the environment
        'POSTGRES_PASSWORD': '${POSTGRES_PASSWORD}', # Password for the DB user from the environment
        'POSTGRES_DB': '${POSTGRES_DB}', # Name of the default DB to create from the environment
    },
    'volumes': ['pgdata:/var/lib/postgresql/data'], # Persist data using a named volume, so it's not lost if the container stops
    'restart': 'always' # Restart the container automatically if it crashes or Docker restarts
}

# If it is in dev mode, set the container port to the local machine
if dev_mode:
    postgres_service['ports'] = ['5432:5432']

# Creating the docker-compose.yml file with or without the dev mode
compose_config = {
    'services': { # Define the services (containers) we want to run
        'postgres': postgres_service # Call the postgres_service dictionary above
    },
    'volumes': { # Define the named volume used above
        'pgdata': {}
    }
}

# Covert the python dictionary into a YAML file and do not sort the dictionary keys (keep it this order)
with open("docker-compose.yml", "w") as f:
    yaml.dump(compose_config, f, sort_keys=False)

print("The docker-compose.yml file was created")


# ----------------------------------------
# 4. Create the requirements.txt file
# ----------------------------------------

print("Fetching requirements.txt from the current environment")

# Will create a new file or overwrite the previous txt file and input the new requirements
with open("requirements.txt", "w") as f:
    # Runs the pip3 freeze command automatically
    subprocess.run(["pip3", "freeze"], stdout=f)
