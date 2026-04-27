# Capitol Trades Bot

An automated scraper and tracker for politician trades on [Capitol Trades](https://www.capitoltrades.com). This bot periodically scans politician profiles, parses their latest trade data, and stores it in a MongoDB database for further analysis or real-time tracking.

## Features

- **Automated Scraping:** Periodically scans the latest politician listing pages.
- **Concurrent Processing:** Uses `asyncio` and `aiohttp` to fetch multiple profile pages concurrently.
- **Smart Updates:** Only updates records when new trades are detected or metadata changes.
- **Recent Trades Stack:** Maintains a capped collection of the most recent trades across all politicians.
- **Robust Error Handling:** Includes timeouts, retries (via logic), and detailed logging.
- **Docker Support:** Ready to be deployed as a containerized service.

## Prerequisites

- **Python 3.12+** (if running locally)
- **MongoDB:** A running MongoDB instance.

## Setup & Installation

### Local Setup

1. **Clone the repository:**
   ```sh
   git clone <repository-url>
   cd capitol-trade-bot
   ```

2. **Create a virtual environment:**
   ```sh
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

4. **Configure environment variables:**
   - Copy the example environment file:
     ```sh
     cp example.env .env
     ```
   - Edit `.env` and provide your `MONGO_URI`.

5. **Run the bot:**
   ```sh
   python -m bot
   ```

### Docker Setup

The project is containerized using Docker and can be run easily with Docker Compose.

1. **Prepare your `.env` file:**
   Ensure you have a `.env` file in the project root with your `MONGO_URI`.

2. **Configure Docker Compose:**
   Uncomment the `env_file` line in `compose.yaml` to pass your environment variables to the container.

3. **Build and start the service:**
   ```sh
   docker compose up --build -d
   ```

## Configuration

The bot's behavior can be tuned in `bot/__main__.py`:

| Variable | Description | Default |
|----------|-------------|---------|
| `SEMAPHORE_LIMIT` | Max concurrent profile page fetches | 10 |
| `MAX_PAGES_TO_SCAN` | Max politician listing pages to check per cycle | 3 |
| `POLLING_INTERVAL_SECONDS` | Time between scan cycles | 3600 (1 hour) |
| `REQUEST_TIMEOUT_SECONDS` | Timeout for HTTP requests | 30 |

## Project Structure

- `bot/`: Main package containing the bot logic.
- `bot/__main__.py`: Entry point and core execution loop.
- `requirements.txt`: Python dependencies.
- `compose.yaml` & `DockerFile`: Docker configuration.
- `example.env`: Template for required environment variables.

---

_Disclaimer: This bot is for educational and research purposes only. Ensure compliance with the target website's Terms of Service and robots.txt._
