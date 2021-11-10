from django.core import serializers
from django.shortcuts import render
from django.http import HttpResponse,JsonResponse
from .models import User,Topic,Post,PostCount
from .forms import PostForm

# Create your views here.
def index(request):
    post_list = Post.objects.all()
    #build a form object
    postForm = PostForm()
    #push post list and form object into the context
    context = {'post_list': post_list, \
        'form': postForm}

    return render(request, 'show_post.html', context)

def uploadPost(request):
    if request.is_ajax and request.method == "POST":
        # get the form data
        form = PostForm(request.POST)
        # save the data and after fetch the object in instance
        if form.is_valid():
            instance = form.save()
            # serialize in new friend object in json
            ser_instance = serializers.serialize('json', [ instance, ])
            # send to client side.
            return JsonResponse({"instance": ser_instance}, status=200)
        else:
            # some form errors occured.
            return JsonResponse({"error": form.errors}, status=400)

def get_post_count(request):
    labels = []
    data = []

    queryset = PostCount.objects.all()

    for entry in queryset:
        labels.append(entry.user_name)
        data.append(entry.post_count)

    return JsonResponse(data={
        'labels': labels,
        'data': data,
    })

def get_barchart(request):
    return render(request, 'barchart.html')
