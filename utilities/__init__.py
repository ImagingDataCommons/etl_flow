from python_settings import settings
import settings as etl_settings

if not settings.configured:
    settings.configure(etl_settings)
assert settings.configured


