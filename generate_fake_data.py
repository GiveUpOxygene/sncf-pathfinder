import csv
import random

# Read sentence templates
with open('sentence_types.txt', 'r', encoding='utf-8') as f:
    sentence_templates = [line.strip() for line in f]

# Read city names from the first column of the CSV, skipping the header
cities = []
with open('gares-de-voyageurs.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=';')
    next(reader)  # Skip header
    for row in reader:
        if row: # check if row is not empty
            cities.append(row[0])

num_lignes = 20000

# Open the output file
with open('fake_data.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['sentence', 'ville_origine', 'ville_arrivee'])

    # Generate num_lignes lines of fake data
    for _ in range(num_lignes):
        # Choose a random sentence template
        template = random.choice(sentence_templates)

        # Choose two different random cities
        ville_origine, ville_arrivee = random.sample(cities, 2)

        # Create the sentence
        sentence = template.replace('[ville origine]', ville_origine).replace('[ville destination]', ville_arrivee)

        # Write the row to the CSV file
        writer.writerow([sentence, ville_origine, ville_arrivee])

print(f"Successfully generated fake_data.csv with {num_lignes} lines.")
