class BaseField(object):

    def __init__(self, default=None, index=False, primary=False):

        self.index = index
        self.primary = primary
        self._value = default

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def serialize(self):
        return self.value

class PrimaryField(object):

    def __init__(self, *args, **kwargs):
        primary = kwargs.pop('primary', '')
        super(BaseField, self).__init__(primary=True, *args, **kwargs)


