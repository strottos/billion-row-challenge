import os

import numpy as np


args = os.sys.argv[1:]

for _ in range(int(args[0])):
    print(f'{np.random.uniform(float(args[1]), float(args[2])):.2f}')
