from django.contrib import admin
from django.urls import include, path
from graphene_django.views import GraphQLView
# import the schema from dashboard
from dashboard.schema import schema

urlpatterns = [
    path('dashboard/', include('dashboard.urls')),
    path('admin/', admin.site.urls),
    path('graphql/', GraphQLView.as_view(graphiql=True, schema=schema)),
    path('scrapy/', include('tasks.urls')),
]
