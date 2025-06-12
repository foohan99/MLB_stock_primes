import requests
import mariadb
import configparser
from datetime import date, datetime
import time

def connect_to_database():
    """Connect to the database with retry logic."""
    config = configparser.ConfigParser()
    config.read('/home/ken/MLB_nuft/config.ini')

    db_config = {
        "host": config["database"]["host"],
        "user": config["database"]["user"],
        "password": config["database"]["password"],
        "database": config["database"]["database"]
    }

    max_retries = 5
    retry_delay = 10

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Attempting to connect to the database (Attempt {attempt}/{max_retries})...")
            connection = mariadb.connect(**db_config)
            print("✅ Successfully connected to the database.")
            return connection
        except mariadb.Error as e:
            print(f"❌ [ERROR] Failed to connect to the database: {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("❌ [ERROR] Maximum retry attempts reached. Exiting.")
                exit(1)

def run_mlb():
    """Fetch and process MLB data."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] Starting MLB script...")

    db = connect_to_database()
    cursor = db.cursor()

    # Get today's date
    today = date.today().isoformat()

    # MLB schedule API endpoint for today
    url = f"https://statsapi.mlb.com/api/v1/schedule?startDate={today}&endDate={today}&sportId=1"

    # Fetch game schedule
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"Error fetching MLB schedule: {e}")
        exit(1)

    # Extract games from the response
    games = data.get("dates", [])[0].get("games", []) if data.get("dates") else []

    # Loop through each game
    for game in games:
        gamePk = game["gamePk"]
        game_date = game["gameDate"][:10]  # Extract the date portion
        game_time = game["gameDate"][11:19]  # Extract HH:MM:SS
        teams = game["teams"]
        home_team = teams["home"]["team"]["name"]
        away_team = teams["away"]["team"]["name"]
        status = game["status"]["detailedState"]

        # Fetch live game linescore
        line_url = f"https://statsapi.mlb.com/api/v1/game/{gamePk}/linescore"
        try:
            line_resp = requests.get(line_url)
            line_resp.raise_for_status()
            line_data = line_resp.json()
        except requests.RequestException:
            continue  # Skip game if linescore is not available

        home_score = line_data.get("teams", {}).get("home", {}).get("runs")
        away_score = line_data.get("teams", {}).get("away", {}).get("runs")
        inning = line_data.get("currentInning")
        inning_state = line_data.get("inningState", "N/A")

        # Insert or update the game data in MariaDB
        sql = """
            INSERT INTO MLB (
                gamePk, game_date, game_time, home_team, away_team,
                home_score, away_score, inning, inning_state,
                status, last_updated
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                home_score=VALUES(home_score),
                away_score=VALUES(away_score),
                inning=VALUES(inning),
                inning_state=VALUES(inning_state),
                status=VALUES(status),
                last_updated=NOW()
        """
        vals = (
            gamePk, game_date, game_time, home_team, away_team,
            home_score, away_score, inning, inning_state,
            status
        )

        try:
            cursor.execute(sql, vals)
            db.commit()
        except mariadb.Error as e:
            print(f"DB error for gamePk {gamePk}: {e}")
            continue

    print(f"Processed {len(games)} games.")
    cursor.close()
    db.close()

    print(f"[{current_time}] Finished MLB script.")

# Run the script only if executed directly
if __name__ == "__main__":
    run_mlb()
