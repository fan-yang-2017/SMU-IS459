from django.shortcuts import render
from django.http import HttpResponse

from .task import start_crawler

# Create your views here.
def index(request):
    return render(request, 'scrapy.html')

def start_scrapy(request):
    start_crawler.delay("https://forums.hardwarezone.com.sg/forums/pc-gaming.382/")
    return HttpResponse("scrapy started")
