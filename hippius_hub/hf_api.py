from typing import Optional
from .auth import get_token, login
from .file_download import hippius_hub_download

class HippiusApi:
    """
    Drop-in replacement for HfApi.
    """
    def __init__(self, token: Optional[str] = None):
        self.token = token or get_token()
        
    def hub_download(self, repo_id: str, filename: str, **kwargs) -> str:
        return hippius_hub_download(repo_id, filename, token=self.token, **kwargs)

class ModelCard:
    """
    Drop-in replacement for ModelCard.
    """
    def __init__(self, text: str):
        self.text = text
