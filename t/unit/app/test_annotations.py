from celery.app.annotations import MapAnnotation, prepare
from celery.utils.imports import qualname


class MyAnnotation:
    foo = 65


class AnnotationCase:

    def setup(self):
        @self.app.task(shared=False)
        def add(x, y):
            return x + y
        self.add = add

        @self.app.task(shared=False)
        def mul(x, y):
            return x * y
        self.mul = mul


class test_MapAnnotation(AnnotationCase):

    def test_annotate(self):
        x = MapAnnotation({self.add.name: {'foo': 1}})
        assert x.annotate(self.add) == {'foo': 1}
        assert x.annotate(self.mul) is None

    def test_annotate_any(self):
        x = MapAnnotation({'*': {'foo': 2}})
        assert x.annotate_any() == {'foo': 2}

        x = MapAnnotation()
        assert x.annotate_any() is None


class test_prepare(AnnotationCase):

    def test_dict_to_MapAnnotation(self):
        x = prepare({self.add.name: {'foo': 3}})
        assert isinstance(x[0], MapAnnotation)

    def test_returns_list(self):
        assert prepare(1) == [1]
        assert prepare([1]) == [1]
        assert prepare((1,)) == [1]
        assert prepare(None) == ()

    def test_evalutes_qualnames(self):
        assert prepare(qualname(MyAnnotation))[0]().foo == 65
        assert prepare([qualname(MyAnnotation)])[0]().foo == 65
