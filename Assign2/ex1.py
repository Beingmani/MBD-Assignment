import math
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint


def s_calc(value, r, b):
    return 1 - math.pow((1 - (math.pow(value, r))), b)


def s_curve(r, b):
    s = np.linspace(0, 1, 100)
    s.astype(int)
    for i in range(len(s)):
        s[i] = s_calc(s[i], r, b)
    return np.array(s)


# values of s
s = np.linspace(0, 1, 100)
pprint(s)


R1 = 3
B1 = 10
R2 = 6
B2 = 20
R3 = 5
B3 = 50


pprint({
    's_curve(R1, B1)': s_curve(R1, B1),
    's_curve(R2, B2)': s_curve(R2, B2),
    's_curve(R3, B3)': s_curve(R3, B3)
})


plt.rcParams["figure.figsize"] = (15, 8.5)
plt.plot(s_curve(R1, B1), marker='.', label='r=3, b=10')
plt.plot(s_curve(R2, B2), marker='.', label='r=6, b=20')
plt.plot(s_curve(R3, B3), marker='.', label='r=5, b=50')
plt.legend()
plt.savefig('s_curve.png', dpi=100)
plt.show()
