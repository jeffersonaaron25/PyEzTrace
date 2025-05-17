
from pyeztrace.tracer import trace

# Class with individually traced methods instead of class decorator
class HelperClass:
    def __init__(self, value):
        self.value = value
        
    @trace()  # Directly trace the method
    def get_value(self):
        return self.value
        
    @trace()  # Directly trace the method
    def double_value(self):
        return self.value * 2
    
    def __str__(self):
        return f"HelperClass({self.value})"

# Functions that will be called and traced
def helper_function(x):
    return x + 10

def another_helper(x):
    return x * 2
