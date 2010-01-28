from django.conf.urls.defaults import *
import views

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
        url(r'^multiply/', views.multiply, name="multiply"),
)
