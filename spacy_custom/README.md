# Création d'un modèle custom avec spacy

- Lancer [prepare_data.py](prepare_data.py)
- Lancer la commande : ```python -m spacy init config config.cfg --lang fr --pipeline ner --optimize efficiency```
- Entraîner le modèle avec : ```python -m spacy train config.cfg --output ./output --paths.train ./train.spacy --paths.dev ./dev.spacy```
