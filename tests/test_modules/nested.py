"""Nested module for testing deep recursive tracing."""

def nested_function():
    """Function in nested module that should be discovered with depth > 1."""
    return 30

class NestedClass:
    """Class in nested module to test class method tracing."""
    
    def method_a(self):
        """Class method that should be traced."""
        return 35
        
    def method_b(self):
        """Another class method."""
        return self.method_a() + 5 