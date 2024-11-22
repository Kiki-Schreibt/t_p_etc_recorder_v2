import numpy as np
from scipy.optimize import differential_evolution
import matplotlib.pyplot as plt

from src.table_data import TableConfig


np.random.seed(42)
x_data = np.linspace(-10, 10, 100)
y_true = 2 * x_data**2 - 1 * x_data + 0.5
y_data = y_true + np.random.normal(0, 10, size=x_data.shape)

def model(x, params):
    a, b, c = params
    return a * x**2 + b * x + c

def objective(params, x, y):
    return np.sum((y - model(x, params))**2)

def bla():
    bounds = [(-10, 10), (-10, 10), (-10, 10)]
    result = differential_evolution(objective, bounds, args=(x_data, y_data))
    best_params = result.x
    print(f"Best-fitting parameters: a={best_params[0]}, b={best_params[1]}, c={best_params[2]}")

    plt.scatter(x_data, y_data, label='Data')
    plt.plot(x_data, model(x_data, best_params), color='red', label='Best Fit')
    plt.legend()
    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Differential Evolution Fit')
    plt.show()



if __name__ == '__main__':
    import os

data = [(20.0, '00:10:00', 1.0, 5.0), (30.0, '00:10:00', 2.0, 4.0), (40.0, '00:10:00', 3.0, 6.0), (20.0, '00:10:00', 1.0, 5.0), (30.0, '00:10:00', 2.0, 4.0), (40.0, '00:10:00', 3.0, 6.0), (20.0, '00:10:00', 1.0, 5.0), (30.0, '00:10:00', 2.0, 4.0), (40.0, '00:10:00', 3.0, 6.0), (20.0, '00:10:00', 1.0, 5.0), (30.0, '00:10:00', 2.0, 4.0), (40.0, '00:10:00', 3.0, 6.0)]
for a in data[1:]:
    temp = a[0]
    time = a[1]
    print(temp, time)
