# Changelog

All notable changes to this project as well as things I have learned will be documented in this file.

---

## 2025-05-05
### Initial Infrastructure Setup (Phase 1)

- Created `init_project.py` to automate the creation of the project environment.
- Added secure `.env` management:
  - Automatically generates `.env.example` for GitHub
  - Prompts for user input to generate a real `.env`
- Integrated Chainguard’s secure PostgreSQL image.
- Dynamically generates `docker-compose.yml` with support for:
  - Dev mode: exposes port `5432`
  - Prod mode: omits port for security
- Added volume persistence using Docker named volumes (`pgdata`)
- Automatically generates `requirements.txt` using `pip3 freeze`
- Installed and configured:
  - `pyyaml`
  - `requests`
  - `subprocess`
  - `getpass`

- Learned:
  - `docker-compose down -v` deletes the persistent DB volume
  - `docker-compose down` should usually be run instead
  - Port exposure should be conditional on environment
  - Running `python3` vs `python` can affect which pip installation is used
- Issues Fixed:
  - `ModuleNotFoundError: No module named 'yaml'` by ensuring correct interpreter and environment (python instead of python3)

---