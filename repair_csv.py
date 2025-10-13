import csv

with open("player_links.csv", "r", encoding="utf-8") as infile, \
     open("player_links_quoted.csv", "w", newline="", encoding="utf-8") as outfile:
    
    reader = csv.reader(infile)
    writer = csv.writer(outfile, quotechar='"', quoting=csv.QUOTE_ALL)
    
    for row in reader:
        writer.writerow(row)

# Optionally replace the original file
import os
os.replace("player_links_quoted.csv", "player_links.csv")
