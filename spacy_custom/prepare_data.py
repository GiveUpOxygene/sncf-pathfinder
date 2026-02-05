import pandas as pd
import spacy
from spacy.tokens import DocBin
from sklearn.model_selection import train_test_split

df = pd.read_csv('../fake_data.csv')

train_df, dev_df = train_test_split(df, test_size=0.2, random_state=42)

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

nlp = spacy.blank("fr")

train_db = create_docbin(train_df, nlp)
train_db.to_disk("./train.spacy")

dev_db = create_docbin(dev_df, nlp)
dev_db.to_disk("./dev.spacy")

print(f"Train: {len(train_df)} exemples")
print(f"Dev: {len(dev_df)} exemples")