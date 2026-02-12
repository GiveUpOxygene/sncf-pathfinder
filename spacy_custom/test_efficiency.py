import pandas as pd
import spacy
from spacy.tokens import DocBin
from sklearn.model_selection import train_test_split
import argparse
import os
import subprocess

def create_docbin(dataframe, nlp):
    db = DocBin()
    for _, row in dataframe.iterrows():
        phrase = row['sentence']
        ville_origine = row['ville_origine']
        ville_arrivee = row['ville_arrivee']

        doc = nlp.make_doc(phrase)
        ents = []

        start_dep = phrase.lower().find(ville_origine.lower())
        if start_dep != -1:
            span = doc.char_span(start_dep, start_dep + len(ville_origine), label="VILLE_ORIGINE")
            if span:
                ents.append(span)

        start_arr = phrase.lower().find(ville_arrivee.lower())
        if start_arr != -1:
            span = doc.char_span(start_arr, start_arr + len(ville_arrivee), label="VILLE_ARRIVEE")
            if span:
                ents.append(span)

        doc.ents = ents
        db.add(doc)
    return db

def main(n_sentences):
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), '..', 'fake_data.csv'))
    df = df.sample(n=n_sentences, random_state=42)

    train_df, dev_df = train_test_split(df, test_size=0.2, random_state=42)

    nlp = spacy.blank("fr")

    # Create train and dev .spacy files
    train_db = create_docbin(train_df, nlp)
    train_db.to_disk("./train.spacy")

    dev_db = create_docbin(dev_df, nlp)
    dev_db.to_disk("./dev.spacy")

    print(f"Data prepared with {n_sentences} sentences.")
    print(f"Train: {len(train_df)} examples")
    print(f"Dev: {len(dev_df)} examples")

    # Generate config file
    config_command = "python -m spacy init config config.cfg --lang fr --pipeline ner --optimize efficiency --force"
    subprocess.run(config_command, shell=True, check=True)

    # Train the model
    output_path = f"./output_{n_sentences}"
    train_command = f"python -m spacy train config.cfg --output {output_path} --paths.train ./train.spacy --paths.dev ./dev.spacy"
    subprocess.run(train_command, shell=True, check=True)

    # Evaluate the model
    evaluate_command = f"python -m spacy evaluate {output_path}/model-best ./dev.spacy"
    result = subprocess.run(evaluate_command, shell=True, check=True, capture_output=True, text=True)

    print(f"\nResults for {n_sentences} sentences:")
    print(result.stdout)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train and evaluate a spaCy model with a specific number of sentences.")
    parser.add_argument("n_sentences", type=int, help="Number of sentences to use for training.")
    args = parser.parse_args()
    main(args.n_sentences)
