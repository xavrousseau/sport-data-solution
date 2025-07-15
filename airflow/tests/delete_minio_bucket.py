# ==========================================================================================
# Script      : scripts/delete_minio_bucket.py
# Objectif    : Supprimer complètement un bucket MinIO (purge tous les objets puis supprime le bucket)
#
# Description :
#   Ce script efface TOUT le contenu du bucket MinIO défini dans la configuration,
#   puis supprime le bucket lui-même. Pratique pour réinitialiser complètement un environnement
#   ou nettoyer après des tests, mais attention : c’est une opération destructive et irréversible !
#
# Prérequis :
#   - Le fichier minio_helper.py doit être accessible et fonctionnel
#   - Les variables d’environnement MINIO_* doivent être définies dans le .env
#
# Utilisation :
#   python scripts/delete_minio_bucket.py
#
# Sécurité :
#   ⚠️ ATTENTION : tout le bucket ET son contenu seront supprimés sans possibilité de récupération !
#
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from minio_helper import MinIOHelper

def main():
    """
    Supprime tous les objets du bucket MinIO puis le bucket lui-même.
    """
    helper = MinIOHelper()
    helper.delete_bucket()

# ------------------------------------------------------------------------------------------
# Point d’entrée : exécution directe uniquement
# ------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()

# ------------------------------------------------------------------------------------------
# Fin du fichier – Utiliser avec précaution !
# ------------------------------------------------------------------------------------------
