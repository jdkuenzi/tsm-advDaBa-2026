import logging
import os
import time
import urllib.request

import orjson
from neo4j import GraphDatabase

# ==========================================
# CONFIGURATION DU LOGGER
# ==========================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURATION DE L'APPLICATION
# ==========================================
NEO4J_IP = os.environ.get("NEO4J_IP", "127.0.0.1")
JSON_URL = os.environ.get("JSON_URL", "http://vmrum.isc.heia-fr.ch/files/test.jsonl")
MAX_NODES = int(os.environ.get("MAX_NODES", "1000"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
LOG_INTERVAL = int(os.environ.get("LOG_INTERVAL", "1000"))


def setup_database(session):
    logger.info("Création des contraintes d'unicité...")
    session.run(
        "CREATE CONSTRAINT Article_id IF NOT EXISTS FOR (a:ARTICLE) REQUIRE a._id IS UNIQUE"
    )
    session.run(
        "CREATE CONSTRAINT Author_id IF NOT EXISTS FOR (a:AUTHOR) REQUIRE a._id IS UNIQUE"
    )


def insert_batch(tx, batch):
    # 1. Insertion des Articles
    tx.run(
        """
        UNWIND $batch AS article
        MERGE (a:ARTICLE {_id: article.id})
        SET a.title = article.title
        """,
        batch=batch,
    )

    # 2. Insertion des Auteurs et relation AUTHORED
    tx.run(
        """
        UNWIND $batch AS article
        UNWIND article.authors AS author
        MERGE (au:AUTHOR {_id: author.id})
        SET au.name = author.name
        WITH au, article
        MATCH (a:ARTICLE {_id: article.id})
        CREATE (au)-[:AUTHORED]->(a)
        """,
        batch=batch,
    )

    # 3. Insertion des relations CITES
    tx.run(
        """
        UNWIND $batch AS article
        MATCH (a1:ARTICLE {_id: article.id})
        UNWIND article.references AS ref
        MERGE (a2:ARTICLE {_id: ref})
        CREATE (a1)-[:CITES]->(a2)
        """,
        batch=batch,
    )


def main():
    logger.info("========== VARIABLES D'ENVIRONNEMENT ==========")
    logger.info(f"NEO4J_IP   : {NEO4J_IP}")
    logger.info(f"JSON_URL   : {JSON_URL}")
    logger.info(f"MAX_NODES  : {MAX_NODES}")
    logger.info(f"BATCH_SIZE : {BATCH_SIZE}")
    logger.info(f"LOG_LEVEL  : {LOG_LEVEL}")
    logger.info(f"LOG_INTERVAL: {LOG_INTERVAL}")
    logger.info("===============================================")
    last_log_count = 0
    uri = f"bolt://{NEO4J_IP}:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))

    connected = False
    logger.info(f"Connexion à Neo4j sur {NEO4J_IP}...")
    while not connected:
        try:
            time.sleep(5)
            driver.verify_connectivity()
            connected = True
            logger.info("Connexion établie avec succès !")
        except Exception:
            logger.warning(
                "Neo4j n'est pas encore prêt. Nouvelle tentative dans 5 secondes..."
            )

    with driver.session() as session:
        setup_database(session)

    start_time = time.time()
    end_time = start_time
    nodes_processed = 0
    batch = []
    logger.info(f"Limite fixée à {MAX_NODES} articles.")

    with driver.session() as session:
        try:
            req = urllib.request.Request(JSON_URL)
            with urllib.request.urlopen(req) as response:
                start_time = time.time()
                logger.info(f"Début de l'insertion, streaming depuis : {JSON_URL}")
                for line_bytes in response:
                    if nodes_processed >= MAX_NODES:
                        break

                    try:
                        data = orjson.loads(line_bytes)

                        batch.append(
                            {
                                "id": data.get("id"),
                                "title": data.get("title"),
                                "authors": data.get("authors", []),
                                "references": data.get("references", []),
                            }
                        )

                    except orjson.JSONDecodeError:
                        line = line_bytes.decode("utf-8").strip()
                        logger.warning(
                            f"Ligne JSON ignorée (malformée) : {line[:50]}..."
                        )
                        continue

                    if len(batch) >= BATCH_SIZE:
                        session.execute_write(insert_batch, batch)
                        nodes_processed += BATCH_SIZE
                        if nodes_processed - last_log_count >= LOG_INTERVAL:
                            logger.info(
                                f"Progression : {nodes_processed} articles streamés et insérés..."
                            )
                            last_log_count = nodes_processed

                        batch = []

                if batch:
                    nodes_processed += len(batch)
                    session.execute_write(insert_batch, batch)

            end_time = time.time()
            elapsed_time = end_time - start_time

        except urllib.error.URLError as e:
            logger.error(f"Erreur fatale lors de l'accès à l'URL : {e}")
        finally:
            logger.info("=============== INSERTION FINIS ===============")

            total_articles = session.run(
                "MATCH (a:ARTICLE) RETURN count(a) as c"
            ).single()["c"]
            total_authors = session.run(
                "MATCH (a:AUTHOR) RETURN count(a) as c"
            ).single()["c"]

            logger.info(f"N (Articles) : {total_articles}")
            logger.info(f"K (Auteurs)  : {total_authors}")
            logger.info(f"Total (N+K)  : {total_articles + total_authors} noeuds")
            logger.info(
                f"start_time : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
            )
            logger.info(
                f"end_time   : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}"
            )
            logger.info(f"Temps de chargement total : {elapsed_time:.2f} secondes")
            logger.info("===============================================")

    driver.close()


if __name__ == "__main__":
    main()
