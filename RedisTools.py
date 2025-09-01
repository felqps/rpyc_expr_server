import redis
from pathlib import Path
import sys
import pickle

class RedisTools(object):
    def __init__(self, fn_cfg):
        self.fn_cfg = fn_cfg
        self.r = None
    
    def get_redis(self):
        if self.r is None:
            if not Path(self.fn_cfg).exists():
                return self.r
            cfg = eval(Path(self.fn_cfg).read_text())
            self.r = redis.Redis(host=cfg["host"], port=cfg["port"], db=cfg["db"])
            if False:
                print(f"Create RedisTools instance, {cfg}", file=sys.stderr)
        return self.r

    def set_value(self, k, v, debug=False):
        if debug:
            if k.find("Rqraw_RqAR_1d_1")>=0:
                print(f"DEBUG_9034: redis set_value k= {k}, v= {v}")
        self.get_redis().set(k, v)

    def get_value(self, k, debug=False):
        v = self.get_redis().get(k)
        if v:
            try:
                v = pickle.loads(v)
            except pickle.UnpicklingError:
                v = v.decode('utf-8')
            if debug:
                if k.find("Rqraw_RqAR_1d_1")>=0:
                    print(f"DEBUG_9033: redis get_value k= {k}, v= {v}")
        return v
    
    def del_value(self, k):
        self.get_redis().delete(k)

my_redis = RedisTools(fn_cfg="/qpsdata/config/redis_server.cfg")
test_redis = RedisTools(fn_cfg="/qpsdata/config/redis_genetic_func_server.cfg")

if __name__ == '__main__':
    fn = "/qpsuser/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/prod1w_20210810_20221230/1d/Pred_Rqraw_RqAR_1d_1/D46.db"
    redis_hkey = f"fld_cache:{fn}"
    redis_hval = "ab4c956fc18a054c1d7bdaff754e54a9"
    my_redis.get_value(redis_hkey, debug=True)
    my_redis.set_value(redis_hkey, redis_hval, debug=True)
    my_redis.get_value(redis_hkey, debug=True)
    print(test_redis.get_value("genetic_func:Resp_C2C.C39.G:95be89a6694da263aaafb461e1b5c1a0.txt"))
    print(my_redis.get_value("genetic_func:Resp_C2C.C39.G:95be89a6694da263aaafb461e1b5c1a0.txt"))
