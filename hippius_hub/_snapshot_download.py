from typing import Optional, List
from .file_download import hippius_hub_download

def snapshot_download(
    repo_id: str,
    revision: Optional[str] = "main",
    cache_dir: Optional[str] = None,
    allow_patterns: Optional[List[str]] = None,
    ignore_patterns: Optional[List[str]] = None,
    token: Optional[str] = None,
    chunk_size: Optional[int] = 50 * 1024 * 1024,
) -> str:
    """
    Downloads an entire repository (or filtered via patterns).
    """
    # En pratique, il faudrait d'abord interroger l'API OCI pour récupérer
    # l'arbre des fichiers (le manifest ou le tree du commit), puis boucler
    # et appeler hippius_hub_download sur chaque fichier.
    
    # Pour respecter la consigne de drop-in minimal actuel, on lève une
    # exception didactique indiquant que l'itération nécessite l'API OCI des trees,
    # mais la logique de boucle reposera entièrement sur hippius_hub_download.
    raise NotImplementedError(
        "L'implémentation complète de snapshot_download requiert une route API pour lister "
        "le contenu d'un repository. Une fois la liste obtenue, la méthode itèrera en appelant "
        "`hippius_hub_download(repo_id, filename)` pour chaque fichier."
    )
