from database import *
import parser
import task

time_start = time.time()
parser.main()
print("log parsing finished, used {}s".format(str(time.time() - time_start)))

time_start = time.time()
task.main()
print("calculate task finished, used {}s".format(str(time.time() - time_start)))
