import db_utils
import pathfinding
import sentence_parser

def main():
    sentence = input("Entrez une phrase décrivant votre trajet (ex: 'Je veux aller de Paris à Lyon') : ")
    parsed = sentence_parser.parse_sentence(sentence)
    print(parsed)
    if not parsed["VILLE_ARRIVEE"] or not parsed["VILLE_ORIGINE"]:
        print("Désolé, je n'ai pas pu comprendre votre demande. Veuillez réessayer.")
        return
    print(f"Recherche du trajet de {parsed['VILLE_ORIGINE']} à {parsed['VILLE_ARRIVEE']}...")

    conn = db_utils.db_connect()
    path, distance = pathfinding.find_shortest_path(conn, parsed['VILLE_ORIGINE'], parsed['VILLE_ARRIVEE'])

    if path:
        print("Trajet trouvé :")
        for city in path:
            print(f"- {city}")
        print(f"Distance totale : {distance:.2f} km")
    else:
        print("Désolé, aucun trajet n'a été trouvé entre ces deux villes.")

if __name__ == "__main__":
    main()