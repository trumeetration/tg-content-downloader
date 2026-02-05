from typing import TypedDict, NotRequired

class LinkResult(TypedDict):
    chat_id: int
    result_video_url: str
    success: bool
    requested_url: str
    error: NotRequired[str]