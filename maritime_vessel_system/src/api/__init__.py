# API package
# Lazy imports to avoid circular import issues
def get_app():
    from .app import app
    return app

def create_app():
    from .app import create_app as _create_app
    return _create_app()
