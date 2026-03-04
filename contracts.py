from typing import TypedDict, NotRequired, List

class ResultContentItem(TypedDict):
    content_path: str
    thumbnail_path: str
    is_video: bool

class LinkResult(TypedDict):
    chat_id: int
    bucket_base_url: str
    content_items: List[ResultContentItem]
    success: bool
    requested_url: str
    error: NotRequired[str]