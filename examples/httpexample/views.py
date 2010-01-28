from django.http import HttpResponse

from anyjson import serialize


def multiply(request):
    x = int(request.GET["x"])
    y = int(request.GET["y"])

    retval = x * y
    response = {"status": "success", "retval": retval}
    return HttpResponse(serialize(response), mimetype="application/json")
