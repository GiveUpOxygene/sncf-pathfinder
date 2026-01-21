# sncf-pathfinder

Ce projet sera scindé en deux parties :
- du NLP (natural language processing) pour déterminer les intentions de l'utilisateur
- de la théorie des graphes pour trouver un chemin optimal entre les deux gares

## To-do list

### NLP

- [x] création d'un jeu de données de types de phrases
- [x] création d'un jeu de données de phrases
- [x] tests de différents modèles
- [ ] fine tuning d'un modèle choisi


### Théorie des graphes

- [ ] récupération des données de la sncf
- [ ] structuration de la base de données
- [ ] création d'un algorithme de pathfinding


## Création de la base de données

Exécuter le script python dans sncf-data

## Sources

[Liste des gares](https://ressources.data.sncf.com/explore/dataset/liste-des-gares/information/)<br/>
[Liste des ligne](https://ressources.data.sncf.com/explore/dataset/lignes-par-type/information/)<br/>
[Vitesse max par ligne](https://ressources.data.sncf.com/explore/dataset/vitesse-maximale-nominale-sur-ligne/information/)<br/>
[Tarifs](https://ressources.data.sncf.com/explore/dataset/tarifs-tgv-inoui-ouigo/information/)<br/>