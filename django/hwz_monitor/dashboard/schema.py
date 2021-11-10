import graphene
from graphene_django import DjangoObjectType
from .models import User,Topic,Post

class UserType(DjangoObjectType):
    class Meta:
        model = User

class TopicType(DjangoObjectType):
    class Meta:
        model = Topic

class PostType(DjangoObjectType):
    class Meta:
        model = Post

class Query(graphene.ObjectType):
    users = graphene.List(UserType)
    topics = graphene.List(TopicType)
    posts = graphene.List(PostType)

    def resolve_users(root, info, **kwargs):
        return User.objects.all()

    def resolve_topics(root, info, **kwargs):
        return Topic.objects.all()

    def resolve_posts(root, info, **kwargs):
        return Post.objects.all()


#Use graphene to compile the query schema
#The output schema is fed to graphene_django 
schema = graphene.Schema(query = Query)
