#
# Storage formats
# ===============
#
# 1. Raw JSON storage (more debuggable):
#
#  Task results will be stored in raw JSON storage by default. To do the same for messages and broadcasts:
#
#  BROKER_TRANSPORT_OPTIONS = {'body_encoding': 'raw'}
#  CELERY_TASK_SERIALIZER = 'raw'
#
# 2. Binary storage (compact, opaque):
#
#  Messages and broadcasts will be stored in binary by default. To do the same for task results:
#
#  CELERY_REST_BACKEND_SETTINGS = {'format': 'base64'}
#

#
# BROKER_TRANSPORT_OPTIONS for "rest:"
# ====================================
#  'body_encoding': if set to None, then you may also want to set CELERY_TASK_SERIALIZER to 'raw'
#  'protocol': 'http', 'https' (default: 'http')

#
# CELERY_REST_BACKEND_SETTINGS
# ============================
#  'protocol': 'http', 'https' (default: 'http')
#  'format': 'json', 'base64' (default: 'json')
#

CELERY_IMPORTS = ('tasks',)

# Messaging
BROKER_URL = 'rest://localhost:8080/messaging'
BROKER_TRANSPORT_OPTIONS = {'body_encoding': 'raw'}
CELERY_TASK_SERIALIZER = 'raw'

# Results
CELERY_RESULT_BACKEND = 'rest://localhost:8080/messaging'
#CELERY_REST_BACKEND_SETTINGS = {'format': 'base64'}
