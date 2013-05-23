class Data(object):
    def __init__(self, label, data):
        self.label = label
        self.data = data
    def __str__(self):
        return 'Data(%s)' % self.label
    __unicode__ = __repr__ = __str__
