from .models import User,Topic,Post
from django import forms

class PostForm(forms.ModelForm):
    class Meta:
        model = Post
        fields = ("__all__")
