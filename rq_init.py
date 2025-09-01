import sys


from QD_USER import *
from common_ctx import *
import QpsUtil

#QD_USER = "che"
QSF_ROOT_DIR = f"{rootdata()}/raw/data_rq"
QCF_ROOT_DIR = f"{rootuser()}/{QD_USER}/data_rq"
hostname = QpsUtil.gethostname()

rq_username="UNKNOWN"
rq_password="UNKNOWN"
if False or hostname in ["che00", "che90"] or QD_USER == 'axiong01': #Trial account
    rq_username = "license"
    rq_password="GQO_1TMffXArrH2zejQxX0JuMmlPVak5MywN4zARltKZhmd2oxwMDFfX7J_dgW12OEstIOCaPwmO62GCDHTW-2b-uQMqcJsprLUbaqxFFU7UL3ew1Rj64yUylmWo_jmz8v4VUlYl_o2Qtsigm9_Upj5z_0qA7PqxljBzZzAc0YU=BjK99uhbbiA9jZvBgbYBoKfEQ5be3_1tQSI1EqpxExZrn0b3Jk3hSWhly1wICDYlN9FIl0tGXT8moZxz7k0NYunQWvyfzh_i3inB6npt7rBE_y7iwEC80J4vDJd4NN-7BFiQdVqQeCOE0i386AP9lhwlhcbBwhUtrkCJ_2NsEDk="

elif hostname in ["che93", "wkr95", "MSI02"]:  #REAL account
    rq_username = "license"
    rq_password = "B8DspY987BTphbPainstiO23lUfjmgZv-UfYX869cLfBPZvoSMQL0yrS7Hys38XUuLJwbcc4Me519mHWm2JeahkVtnpPn-Qo7oc7IR0q8oBW454omtQN0gHxu6X6hT7tcQ-zbwQlXk2uLzYw91jSP-GcyM0c3-OBY8vuA7o0zwg=HhyF14IkdIdFhEfSJDBn60uKF2hZF4_EmxV5wyBRs1Dc43x3v4q9nqMxsBDay4-w__iAYJ6M-cx3F1GuebYCkgA9CHeQCJitAWIekG5J-FpcdqF9a91qBVBlP_JWbjQG3VfwHMaWn6xjfOPxwQvOtGIqOWe3qJ4GqZRBgtl4YoE="

else:
    pass
    #assert False, f"ERROR: rq_init() not allowed for host/user"

#print(f"INFO: rq_password= {rq_password[:6]}")

