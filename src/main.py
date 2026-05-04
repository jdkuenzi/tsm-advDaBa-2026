import logging
import os
import time
from itertools import batched

import orjson
import requests
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
NEO4J_AUTH = os.environ.get("NEO4J_AUTH", "neo4j/neo4j")
MAX_NODES = int(os.environ.get("MAX_NODES", "1000"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "1000"))
LOG_INTERVAL = int(os.environ.get("LOG_INTERVAL", "1000"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))


def setup_database(session):
    logger.info("Création des contraintes d'unicité...")
    session.run(
        "CREATE CONSTRAINT Article_id IF NOT EXISTS FOR (a:ARTICLE) REQUIRE a._id IS UNIQUE"
    )
    session.run(
        "CREATE CONSTRAINT Author_id IF NOT EXISTS FOR (a:AUTHOR) REQUIRE a._id IS UNIQUE"
    )


def insert_batch(tx, batch):
    tx.run(
        """
        UNWIND $batch AS article
        MERGE (a:ARTICLE {_id: article.id})
        SET a.title = article.title
        """,
        batch=batch,
    )

    citations_edges = []
    for article in batch:
        src_id = article.get("id")
        if not src_id:
            continue
        for ref_id in article.get("references", []):
            if ref_id:
                citations_edges.append({"src": src_id, "tgt": ref_id})

    for i in range(0, len(citations_edges), CHUNK_SIZE):
        chunk = citations_edges[i : i + CHUNK_SIZE]
        tx.run(
            """
            UNWIND $chunk AS edge
            MATCH (src:ARTICLE {_id: edge.src})
            MERGE (tgt:ARTICLE {_id: edge.tgt})
            CREATE (src)-[:CITES]->(tgt)
            """,
            chunk=chunk,
        )

    authors_edges = []
    for article in batch:
        article_id = article.get("id")
        if not article_id:
            continue
        for author in article.get("authors", []):
            if author and author.get("id"):
                authors_edges.append(
                    {
                        "article_id": article_id,
                        "author_id": author.get("id"),
                        "author_name": author.get("name"),
                    }
                )

    for i in range(0, len(authors_edges), CHUNK_SIZE):
        chunk = authors_edges[i : i + CHUNK_SIZE]
        tx.run(
            """
            UNWIND $chunk AS edge
            MATCH (a:ARTICLE {_id: edge.article_id})
            MERGE (au:AUTHOR {_id: edge.author_id})
            SET au.name = edge.author_name
            CREATE (au)-[:AUTHORED]->(a)
            """,
            chunk=chunk,
        )


def wait_for_neo4j(driver):
    logger.info(f"Connexion à Neo4j sur {NEO4J_IP}...")
    while True:
        try:
            driver.verify_connectivity()
            logger.info("Connexion établie avec succès !")
            break
        except Exception:
            logger.warning("Neo4j n'est pas encore prêt. Nouvelle tentative dans 5s...")
            time.sleep(5)


def stream_articles(url, byte_offset, max_nodes):
    headers = requests.utils.default_headers()
    if byte_offset > 0:
        headers["Range"] = f"bytes={byte_offset}-"

    with requests.get(url, headers=headers, stream=True, timeout=(60, 600)) as response:
        response.raise_for_status()

        current_byte = byte_offset
        nodes_yielded = 0

        for line in response.iter_lines():
            if nodes_yielded >= max_nodes:
                return

            current_byte += 1
            if line:
                current_byte += len(line)
                try:
                    data = orjson.loads(line)
                    yield (
                        {
                            "id": data.get("id"),
                            "title": data.get("title"),
                            "authors": data.get("authors", []),
                            "references": data.get("references", []),
                        },
                        current_byte,
                    )

                    nodes_yielded += 1

                except orjson.JSONDecodeError as e:
                    logger.warning(f"Erreur de décodage JSON ignorée: {e}")
                    continue


def main():
    logger.info("========== VARIABLES D'ENVIRONNEMENT ==========")
    logger.info(f"NEO4J_IP   : {NEO4J_IP}")
    logger.info(f"JSON_URL   : {JSON_URL}")
    logger.info(f"MAX_NODES  : {MAX_NODES}")
    logger.info(f"BATCH_SIZE : {BATCH_SIZE}")
    logger.info(f"CHUNK_SIZE : {CHUNK_SIZE}")
    logger.info(f"LOG_LEVEL  : {LOG_LEVEL}")
    logger.info(f"LOG_INTERVAL: {LOG_INTERVAL}")
    logger.info(f"MAX_RETRIES : {MAX_RETRIES}")
    logger.info("===============================================")

    neo4j_cred = NEO4J_AUTH.split("/")

    driver = GraphDatabase.driver(
        f"bolt://{NEO4J_IP}:7687", auth=(neo4j_cred[0], neo4j_cred[1])
    )
    wait_for_neo4j(driver)

    with driver.session() as session:
        setup_database(session)

    nodes_processed = 0
    retry_count = 0
    last_log_count = 0
    current_byte_offset = 0
    start_time = time.time()

    with driver.session() as session:
        logger.info(f"Streaming depuis : {JSON_URL}")
        start_time = time.time()
        last_log_time = start_time
        while retry_count < MAX_RETRIES and nodes_processed < MAX_NODES:
            try:
                if nodes_processed > 0:
                    logger.info(
                        f"Reprise du flux : on ignore les {nodes_processed} premiers noeuds ({current_byte_offset} bytes)."
                    )
                article_stream = stream_articles(
                    JSON_URL, current_byte_offset, MAX_NODES - nodes_processed
                )
                for batch_tuple in batched(article_stream, BATCH_SIZE):
                    retry_count = 0
                    batch = [item[0] for item in batch_tuple]

                    session.execute_write(insert_batch, batch)
                    nodes_processed += len(batch)
                    current_byte_offset = batch_tuple[-1][1]

                    if nodes_processed - last_log_count >= LOG_INTERVAL:
                        current_time = time.time()

                        nodes_inserted = nodes_processed - last_log_count
                        elapsed_time = current_time - last_log_time
                        current_rate = (
                            nodes_inserted / elapsed_time if elapsed_time > 0 else 0
                        )

                        logger.info(
                            f"Progression : {nodes_processed} articles streamés et insérés | Vitesse : {current_rate:.2f} art/s"
                        )
                        last_log_count = nodes_processed
                        last_log_time = current_time
                break

            except (
                requests.RequestException,
                ConnectionError,
                ConnectionAbortedError,
            ) as e:
                logger.warning(f"Coupure réseau détectée : {e}.")
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    logger.info(
                        f"Nouvelle tentative dans 5 secondes... ({retry_count}/{MAX_RETRIES})"
                    )
                    time.sleep(5)
                else:
                    logger.error("Nombre maximum de tentatives atteint.")
                    break

        end_time = time.time()
        elapsed_time = end_time - start_time
        avg_rate = nodes_processed / elapsed_time if elapsed_time > 0 else 0
        logger.info("=============== INSERTION FINIS ===============")
        total_articles = session.run("MATCH (a:ARTICLE) RETURN count(a) as c").single()[
            "c"
        ]
        total_authors = session.run("MATCH (a:AUTHOR) RETURN count(a) as c").single()[
            "c"
        ]
        logger.info(f"Nombre total de noeuds lu en streaming : {nodes_processed}")
        logger.info(f"Vitesse moyenne : {avg_rate:.2f} art/s")
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
