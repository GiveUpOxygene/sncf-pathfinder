import spacy

nlp = spacy.load("../spacy_custom/output/model-best")

def parse_sentence(sentence):
    doc = nlp(sentence)
    arrivee = None
    origine = None
    for ent in doc.ents:
        if ent.label_ == "VILLE_ARRIVEE":
            arrivee = ent.text
        elif ent.label_ == "VILLE_ORIGINE":
            origine = ent.text
    return {"VILLE_ARRIVEE": arrivee, "VILLE_ORIGINE": origine}