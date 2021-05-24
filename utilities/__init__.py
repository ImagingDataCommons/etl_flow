from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured


