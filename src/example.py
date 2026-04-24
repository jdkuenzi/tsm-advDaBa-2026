import os
import time

from neo4j import GraphDatabase


def create_person_tx(tx):
    tx.run("CREATE (p:Person {name: $name, age: $age})", name="Alice", age=30)


def main():
    json_path = os.environ.get("JSON_FILE")
    print(f"Path to JSON file is {json_path}")

    max_nodes_env = os.environ.get("MAX_NODES", "1000")
    nb_articles = max(1000, int(max_nodes_env))
    print(f"Number of articles to consider is {nb_articles}")

    neo4j_ip = os.environ.get("NEO4J_IP")
    print(f"IP addresss of neo4j server is {neo4j_ip}")

    uri = f"bolt://{neo4j_ip}:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))

    connected = False
    while not connected:
        try:
            print("Sleeping a bit waiting for the db")
            time.sleep(5)

            driver.verify_connectivity()
            connected = True
        except Exception:
            pass

    if json_path and os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as fr:
            print("Reading first lines of the json file :")
            for _ in range(5):
                line = fr.readline()
                if not line:
                    break
                print(line, end="")
    else:
        print(f"File not found: {json_path}")

    with driver.session() as session:
        session.execute_write(create_person_tx)

    driver.close()


if __name__ == "__main__":
    main()
