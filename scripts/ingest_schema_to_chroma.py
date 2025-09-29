import os, sys, argparse

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from services.schema_provider import SchemaProvider
from services.chroma_repository import ChromaRepository

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingesta del esquema a Chroma.")
    parser.add_argument("--context", default="database_context.json", help="Ruta al database_context.json")
    parser.add_argument("--chroma_path", default="./chroma_data", help="Ruta de almacenamiento de Chroma")
    parser.add_argument("--collection", default="schema_docs", help="Nombre de colección")
    args = parser.parse_args()

    provider = SchemaProvider(args.context)
    provider.load()
    docs = provider.to_documents()

    repo = ChromaRepository(path=args.chroma_path, collection_name=args.collection)
    repo.upsert_documents(docs)

    print(f"Ingestadas {len(docs)} tablas en Chroma.")

if __name__ == "__main__":
    main()
