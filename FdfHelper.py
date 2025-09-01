import glob

class FdfHelper:
    def __init__(self, fn):
        self._fn = fn
    
    def scn(self, scn_new):
        if len(self._fn.split('/'))<4:
            return self
        if scn_new in ['F']:
            self._fn = self._fn.replace(self._fn.split('/')[-4], "F_20100101_20210101").replace(self._fn.split('/')[3],"data_rq.20100101_20210101")

        if scn_new in ['G']:
            self._fn = self._fn.replace(self._fn.split('/')[-4], "G_20200101_20220101").replace(self._fn.split('/')[3],"data_rq.20200101_20220101")

        if scn_new in ['prod', 'prod1w', 'W']:
            self._fn = glob.glob(self._fn.replace(self._fn.split('/')[-4], "prod1w_20210810_*").replace(self._fn.split('/')[3], "data_rq.20210810_uptodate"))[-1]

        if scn_new in ['T']:
            self._fn = glob.glob(self._fn.replace(self._fn.split('/')[-4], "T_20210810_*").replace(self._fn.split('/')[3], "data_rq.20210810_uptodate"))[-1] #get latest

        return self
    
    def symset(self, symset_new):
        if len(self._fn.split('/'))<4:
            return self
        self._fn = self._fn.replace(self._fn.split('/')[-1], f"{symset_new}.db")
        return self

    def __str__(self):
        return(self._fn)
    
    def fn(self):
        return(self._fn)

    

if __name__ == "__main__":
    funcn = "FdfHelper.main"

    print(FdfHelper("/qpsuser/che/data_rq.20200101_20220101/CS/SN_CS_DAY/G_20200101_20220101/1d/Pred_univ_TOP1800_1d_1/A01.db").scn('F').symset('C25').fn())
    
    print(len("/qpsuser/che/data_rq.20200101_20220101/CS/SN_CS_DAY/G_20200101_20220101/1d/Pred_univ_TOP1800_1d_1/A01.db".split('/')))
