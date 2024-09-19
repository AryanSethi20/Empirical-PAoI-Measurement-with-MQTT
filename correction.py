import numpy as np
import datetime
from scipy import io as spio

PAoI = []
with open("./empirical_results_lambda=1/CU_PAoI-1.txt", 'r') as paoi_logfile:
    for line in paoi_logfile:
        PAoI.append(float(line.split(":")[1].strip("\n")))
    PAoI.pop(0)
    paoi_logfile.close()

# Date and time format
dateFormat = '%Y-%m-%d'
timeFormat = '%H-%M-%S.%f'

PAoI = np.array(PAoI[1000:])
print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Mean PAoI: {:.4f}s".format(np.mean(PAoI)))
print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Variance: {:.4f}".format(np.var(PAoI)))
print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Std Dev: {:.4f}".format(np.std(PAoI)))
spio.savemat("./empirical_results_lambda=1/CU_PAoI-1.mat",\
    {
        'PAoI': PAoI
    })