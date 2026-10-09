# Lab Kafka — livre Gutenberg

Consignes : https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/04.kafka_overview/lab.md

## Exécution sous Windows / PowerShell

Prérequis : Python 3.11+ et Docker Desktop démarré avec son moteur Linux.
Depuis ce dossier :

```powershell
powershell -ExecutionPolicy Bypass -File .\run_lab.ps1
```

Le script installe les dépendances dans `.venv`, télécharge le livre si nécessaire,
démarre Kafka, crée le topic, publie le livre, lance le consommateur et vérifie
le résultat intégral. Un nouvel identifiant d'exécution permet de le relancer
sur le même topic sans mélanger les livres des exécutions précédentes.

Pour exécuter les étapes séparément :

```powershell
.\.venv\Scripts\Activate.ps1
docker compose up -d
python admin.py
python producer.py --run-id lab-001
python consumer.py --run-id lab-001
python verify_output.py
```

Utiliser un nouvel identifiant à chaque envoi. Le consommateur peut aussi tourner
dans un deuxième terminal avant le producteur, avec le même `--run-id`.
Il lit depuis le début avec un nouveau groupe, même s'il démarre après l'envoi.
Sans `--run-id`, il traite la première exécution disponible dans le topic.

Résultats :

- `output/cleaned_book.txt` : texte nettoyé, une ligne non vide par message utile.
- `output/cleaned_book_word_counts.csv` : mots et fréquences, ordre décroissant,
  puis alphabétique pour les égalités.
- `output/cleaned_book_summary.json` : nombres de lignes, tokens et mots distincts.

Arrêter le conteneur du lab : `docker compose down`.
Le broker n'a pas de volume persistant : `down` supprime ses messages.

## Réponses aux étapes du lab

1. Les exemples du cours sont conservés dans `examples/`. L'administrateur crée
   le topic `timer`, le producteur y envoie l'heure pendant cinq minutes, et le
   consommateur décode les messages UTF-8. `flush()` attend la livraison des
   messages en attente. Le nouveau `admin.py` attend aussi le résultat de la
   création et traite le cas d'un topic déjà existant.
2. Le nouveau topic est `gutenberg-book-lines`, avec une partition et un réplica.
   Une partition conserve l'ordre des lignes dans cette démonstration locale.
   Un réplica suffit pour le broker unique, sans tolérance à sa panne.
3. Le livre choisi est *Around the World in Eighty Days*, Jules Verne,
   version anglaise, Project Gutenberg nº 103 : https://www.gutenberg.org/ebooks/103.
   Le fichier UTF-8 original est conservé dans `data/`.
4. `producer.py` lit le fichier ligne par ligne, y compris les lignes vides.
   La valeur Kafka est le texte original de la ligne sans son saut de ligne.
   La clé est son numéro. Les en-têtes identifient l'exécution et le type du
   message. Le producteur attend toutes les livraisons avant d'envoyer un
   message final contenant le nombre de lignes. Ce message final est une
   métadonnée de contrôle supplémentaire, pas une ligne du livre.
5. `consumer.py` s'abonne au nouveau topic, filtre l'exécution demandée et
   vérifie l'ordre, les numéros et le total reçu. Le message final permet de
   terminer explicitement plutôt que d'interpréter un silence comme une fin.
6. `text_cleaning.py` normalise Unicode, passe en minuscules, extrait les mots,
   élimine nombres, ponctuation et mots vides anglais, puis rassemble les
   tokens avec un espace. Les lignes sans tokens sont omises du fichier final.
   Une petite liste explicite de mots vides évite une dépendance NLP.
   Les fréquences complètent l'exercice en reprenant l'idée du word-count.

Exemple : `The HELLO, world! 123 -- and café.` devient `hello world café`.
Les apostrophes et tirets séparent les mots ; `Don't` devient `don` après
suppression de `t`. C'est un nettoyage simple, sans lemmatisation.
L'en-tête et la licence Gutenberg passent aussi dans Kafka et le nettoyage,
puisque le lab demande l'envoi de chaque ligne du fichier téléchargé.

## Vérification

`verify_output.py` compare toutes les lignes du résultat au nettoyage du livre
source et contrôle les compteurs. Une exécution ne réussit donc que si le livre
complet a traversé Kafka. Le consommateur écrit d'abord dans un fichier
temporaire et ne remplace le résultat précédent qu'après réception complète.

Les scripts sont une démonstration locale : les groupes consommateurs sont
jetables et les offsets ne sont pas sauvegardés pour une reprise en production.

## Résultat vérifié le 9 octobre 2026

Exécution réelle avec Python 3.11, confluent-kafka 2.12.2 et
`apache/kafka-native:4.1.1`, sur le broker `localhost:9092`.

| Mesure | Résultat |
| --- | ---: |
| Lignes du livre envoyées et reçues | 8 312 |
| Lignes nettoyées non vides | 6 401 |
| Mots conservés | 37 795 |
| Mots distincts | 6 968 |

Identifiant : `67662649-f67b-484a-ac3d-4c9f14a3ff95`.
La vérification compare chaque ligne nettoyée au livre source : succès,
sans ligne manquante ni différence. Les contrôles sur le nettoyage et les
interfaces des trois scripts ont également réussi.
Le topic contient les lignes originales et un message final de contrôle.
Le conteneur Kafka reste démarré pour permettre l'inspection du lab.

