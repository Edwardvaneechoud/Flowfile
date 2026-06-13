class FlowfileHTTPException(Exception):
    """HTTP-style error raised by engine/secret code without importing FastAPI.

    A handler registered in ``flowfile_core.main`` maps this to the same JSON
    response FastAPI's built-in ``HTTPException`` produces, so route behaviour is
    unchanged while keeping ``flowfile_core.flowfile`` importable without FastAPI.
    """

    def __init__(self, status_code: int, detail: str | None = None):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)
