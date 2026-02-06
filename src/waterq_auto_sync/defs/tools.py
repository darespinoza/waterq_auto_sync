import math

def coerse_float(input):
    try:
        float_value = float(input)
        return float_value
    except ValueError:
        return math.nan
