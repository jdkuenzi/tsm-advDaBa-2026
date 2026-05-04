# TSM Advanced Database - Neo4j TP02

Application pour charger et analyser le réseau de citations DBLP dans une base de données Neo4j.

## Structure du projet

```bash
.
├── src/
│   └── main.py                 # Code principal (stream et charge les données)
├── kube/                       # Configuration Kubernetes
│   ├── secret.yaml             
│   ├── configmap.yaml         
│   ├── neo4j-pvc.yaml          
│   ├── neo4j-statefulset.yaml
│   ├── services.yaml
│   └── job.yaml
├── neo4j_mount/
│   └── conf/                   # Configuration Neo4j (défaut)
├── docker-compose.yaml         # Configuration Docker local
├── Dockerfile                  # Image Docker custom
└── requirements.txt            # Dépendances Python
```

## Lancement en local

### Prérequis

- Docker et Docker Compose installés
- Build l'image docker en local ou pull `jdkuenzi/neo4jtp:latest`
- La base de données doit être vide (supprimer tous les noeuds et les relations)

### Étapes

1. **Démarrer les services (Neo4j + App)**

    ```bash
    docker compose up -d
    ```

2. **Vérifier que tout fonctionne**

   ```bash
   docker compose ps
   ```

   Les deux services `db` et `app` doivent être `Up`.

3. **Voir les logs de l'application**

   ```bash
   docker compose logs -f app
   ```

4. **Accéder à Neo4j**

   - Browser : http://localhost:7474
   - Identifiant : voir [docker-compose.yaml](docker-compose.yaml)
   - Mot de passe : voir [docker-compose.yaml](docker-compose.yaml)

5. **Arrêter les services**

   ```bash
   docker compose down
   ```

Pour monitorer les stats des containers docker

```bash
docker compose stats
```

## Lancement sur Kubernetes

Voir [kube/README.md](kube/README.md)

## Configuration

Les variables d'environnement principales (dans `docker-compose.yaml` et `kube/configmap.yaml`) :

| Variable | Description | Défaut |
|----------|-------------|--------|
| `NEO4J_IP` | Adresse IP de Neo4j | `127.0.0.1` |
| `NEO4J_AUTH` | Identifiant Neo4j | `neo4j/neo4j` |
| `JSON_URL` | URL du fichier de données | [test.jsonl](http://vmrum.isc.heia-fr.ch/files/test.jsonl) |
| `MAX_RETRIES` | Nombre max de tentative successive pour se connecter au flux | `5` |
| `MAX_NODES` | Nombre max de noeuds à charger | `1000` |
| `BATCH_SIZE` | Taille des batch pour le stream | `1000` |
| `CHUNK_SIZE` | Taille des insertion en base pour les auteurs et citations | `1000` |
| `LOG_INTERVAL` | Interval entre les logs de progression | `1000` |
| `LOG_LEVEL` | Niveau de log | `INFO` |

## Données

L'application télécharge le réseau de citations [DBLP-Citation-network-V18.jsonl](http://vmrum.isc.heia-fr.ch/files/DBLP-Citation-network-V18.jsonl)
