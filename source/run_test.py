from pyspark.sql.types import *  
from pyspark.sql.functions import *

import time



with open("test.txt", "a") as myfile:
    myfile.write("appended text")