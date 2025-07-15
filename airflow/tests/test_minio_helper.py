# ==========================================================================================
# Fichier     : tests/test_minio_helper.py
# Objet       : Vérification rapide de la connexion MinIO et du listing d’objets dans le bucket
#
# Description :
#   Ce script permet de tester que la connexion à MinIO fonctionne (via minio_helper.py)
#   et d’afficher la liste des objets actuellement présents dans le bucket configuré.
#   Pratique pour valider rapidement l’accès réseau, les credentials et la configuration .env.
#
# Prérequis :
#   - Le fichier minio_helper.py doit être présent et bien configuré dans le PYTHONPATH
#   - Le fichier .env avec les variables MinIO doit être accessible et complet
#
# Utilisation :
#   python tests/test_minio_helper.py
#
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from minio_helper import MinIOHelper

def main():
    """
    Teste la connexion à MinIO et affiche la liste des objets dans le bucket configuré.
    """
    helper = MinIOHelper()
    print("✅ Connexion à MinIO réussie :", helper.endpoint_url)
    objets = helper.list_objects()
    print("📦 Objets présents dans le bucket :", objets)

# ------------------------------------------------------------------------------------------
# Point d’entrée : exécution directe uniquement pour ce test
# ------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()

# ------------------------------------------------------------------------------------------
# Fin du fichier – Utilisé pour un test manuel rapide de la connexion MinIO
# ------------------------------------------------------------------------------------------
