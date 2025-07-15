# ==========================================================================================
# Fichier     : scripts/cleanup_minio.py
# Objet       : Supprimer tous les objets présents dans un bucket MinIO (vider le bucket)
#
# Description :
#   Ce script vide entièrement un bucket MinIO en supprimant chaque objet qu’il contient.
#   Pratique pour remettre un bucket à zéro en phase de développement, de test ou avant une
#   nouvelle importation massive.
#
# Prérequis :
#   - Le fichier minio_helper.py doit être accessible et correctement configuré
#   - Les variables d’environnement nécessaires à la connexion MinIO doivent être dans .env
#   - Le bucket à vider doit être défini dans le .env ou via le helper (MINIO_BUCKET_NAME)
#
# Utilisation :
#   python scripts/cleanup_minio.py
#
# Sécurité :
#   ⚠️ Cette opération est IRREVERSIBLE ! Toutes les données du bucket seront supprimées.
#
# Auteur      : Xavier Rousseau | Juillet 2025
# ==========================================================================================

from minio_helper import MinIOHelper

def main():
    """
    Vide le bucket MinIO indiqué dans la configuration en supprimant chaque objet.
    """
    helper = MinIOHelper()
    
    # Récupérer la liste des objets à supprimer
    objets = helper.list_objects()
    print(f"🧺 Suppression de {len(objets)} fichiers dans le bucket '{helper.bucket}'")
    
    # Suppression de chaque objet un par un
    for obj in objets:
        helper.delete(obj)
    
    print("✅ Bucket vidé avec succès.")

# Point d'entrée du script
if __name__ == "__main__":
    main()

# ------------------------------------------------------------------------------------------
# Fin du fichier - à lancer pour vider entièrement le bucket MinIO configuré
# ------------------------------------------------------------------------------------------
