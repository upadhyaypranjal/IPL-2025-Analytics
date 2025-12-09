import pandas as pd
import mysql.connector

# Load CSV
df = pd.read_csv("../data_raw/ipl_2025_deliveries.csv")

# Convert NaN to None for MySQL
df = df.where(pd.notnull(df), None)

# Convert date field properly
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date

# MySQL Connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Clares10@",  # your MySQL password
    database="ipl2025"
)

cursor = conn.cursor()

# Insert rows
for row in df.itertuples(index=False):
    cursor.execute("""
        INSERT INTO deliveries_2025
        (match_id, season, phase, match_no, date, venue, batting_team, bowling_team,
         innings, `over`, striker, bowler, runs_of_bat, extras, wide, legbyes, byes,
         noballs, wicket_type, player_dismissed, fielder)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))

conn.commit()
conn.close()

print("🎯 Data inserted successfully into MySQL!")
