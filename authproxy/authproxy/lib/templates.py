from starlette.templating import Jinja2Templates

from authproxy.settings import settings

templates = Jinja2Templates(directory=settings.BASE_DIR / "authproxy" / "templates")
