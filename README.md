## Running the Project with Docker

This project is containerized using Docker and can be run easily with Docker Compose. Below are the project-specific instructions and requirements:

### Requirements
- **Python Version:** The Dockerfile uses `python:3.12-slim` as the base image.
- **Dependencies:** All Python dependencies are specified in `requirements.txt` and are installed into a virtual environment during the build process.

### Environment Variables
- The project supports environment variables via a `.env` file. If you have environment-specific settings, create a `.env` file in the project root. Uncomment the `env_file` line in the `docker-compose.yml` to enable this.

### Build and Run Instructions
1. **Build and start the service:**
   ```sh
   docker compose up --build
   ```
   This will build the image using the provided `DockerFile` and start the `bot` service.

2. **Environment Variables:**
   - If your application requires environment variables, ensure they are defined in a `.env` file at the project root.
   - Uncomment the `env_file: ./.env` line in the `docker-compose.yml` if you need to pass these variables to the container.

### Special Configuration
- The application runs as a non-root user (`appuser`) for improved security.
- The entrypoint runs the bot via `python -m bot`.
- No external services (such as databases) or persistent volumes are configured by default. If needed, add them to the `docker-compose.yml`.

### Ports
- **No ports are exposed by default.** If your bot needs to expose a port, uncomment and configure the `ports` section in the `docker-compose.yml`.

---

_If you update dependencies or the application code, rebuild the image with `docker compose build` before restarting the service._