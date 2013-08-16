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
    
