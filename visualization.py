import pandas as pd
import matplotlib.pyplot as plt



df = pd.read_csv("players_cleaned.csv", sep=None, engine='python', on_bad_lines='skip')

df["Weight"] = (
    df["Weight"]
    .str.replace("kg", "", regex=False)
    .str.strip()
    .astype(float)
)

average_weight_by_position = (
    df.groupby("Position")["Weight"]
    .mean()
    .sort_values(ascending=False)
    .head(10)
)

plt.figure(figsize=(10, 5))
plt.barh(average_weight_by_position.index, average_weight_by_position.values)
plt.xlabel("Durchschnittsgewicht (kg)", fontsize=12)
plt.title("Durchschnittsgewicht nach Spielerposition", fontsize=14, fontweight="bold")
plt.gca().invert_yaxis()
plt.grid(axis="x", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()

players_per_country = (
    df["Birth Country"]
    .value_counts()
    .head(10)
)

plt.figure(figsize=(10, 5))
plt.barh(players_per_country.index, players_per_country.values)
plt.xlabel("Anzahl der Spieler", fontsize=12)
plt.title("Top 10 Länder nach Spieleranzahl", fontsize=14, fontweight="bold")
plt.gca().invert_yaxis()
plt.grid(axis="x", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()


df_non_us = df[df["Birth Country"].str.strip().str.lower() != "us"]

players_per_country_non_us = (
    df_non_us["Birth Country"]
    .value_counts()
    .head(10)
)

plt.figure(figsize=(10, 5))
plt.barh(players_per_country_non_us.index, players_per_country_non_us.values)
plt.xlabel("Anzahl der Spieler", fontsize=12)
plt.title("Top 10 Länder (außer USA) nach Spieleranzahl", fontsize=14, fontweight="bold")
plt.gca().invert_yaxis()
plt.grid(axis="x", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()