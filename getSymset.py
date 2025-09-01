
#!/home/shuser/anaconda3/bin/python
import sys

import os
from QDFile import *
from common_symsets import *
#from QDF import *

from ssn2mkts import ssn2mkts
from inactive_CF import inactiveMkts

#ssn = symset name
ssn2mkts = None
def get_ssn2mkts():
    #Return a list of mkt for the symset
    #For stocks, it is a list of ticker symbols.
    #For futures, it is a list of roots which will be mapped to the main contract dynamically
    #ssn2mkts = {}
    global ssn2mkts
    if ssn2mkts == None:
        #qps_print(f'importing ssn2mkts')
        from ssn2mkts import ssn2mkts
    return (ssn2mkts)

def get_futs_roots(regen=False):
    if regen:
        inFn = QDFile(fp=dd('raw')).label('futures').label('all_instruments.db')
        assert os.path.exists(inFn.fp()), f'ERROR: cannot find {inFn}'
        dp = inFn.load()
        futs = dp['Future']
        return(sorted(list(set(futs['underlying_symbol']))))
    else:
        return(get_ssn2mkts()['CF_ALL'])

def get_ssns(symset):
    if symset == 'CF':
        #return [x for x in cn_fut_ssns() if x.find('TEST')<0]
        return [x for x in cn_fut_ssns()]
    elif symset == 'CF_DS':
        return [x for x in cn_fut_ssns() if x.find('TEST')>=0 or x.find('_NS_')<0]
    elif symset == 'CF_NS':
        return [x for x in cn_fut_ssns() if x.find('TEST')>=0 or x.find('_NS_')>=0]
    elif symset == 'CF_TEST':
        return [x for x in cn_fut_ssns() if x.find('TEST')>=0]
    elif symset == 'CS':
        return cn_stk_ssns()
    elif symset == 'ALL':
        return cn_fut_ssns() + cn_stk_ssns()
    else:
        return [symset]
    
def cn_fut_ssns():
    ssn2mkts = get_ssn2mkts()
    return sorted(list(get_ssn2mkts().keys()))

def cn_stk_ssns():
    return sorted(['CS_TEST01', 'D45', 'CS_ZZ500', 'CS_LOWOPN', 'CS_ALL'])

def cn_stk_indus():
    return cnstk_indus()

def getSymset(univname):
    ssn2mkts = get_ssn2mkts()
    if univname in ssn2mkts:
        mktList = ssn2mkts[univname]
    else:
        mktList = univname.split(",")

    inactiveMkts = None
    from inactive_CF import inactiveMkts

    # for mkt in inactiveMkts:
    #     if mkt in mktList:
    #         mktList.remove(mkt)    
    return(sorted(mktList))

def str2ssn_list(symset, plus_all=False):
    if symset.find("'CF_MKTS'") >= 0:
        symsetStr = symset.replace('CF_MKTS', ','.join(getSymset('CF_ALL_MKTS')))
    elif symset.find("CF_GRPS") >= 0:
        symsetStr = ','.join(cn_fut_ssns())
    elif symset in ("CS_ALL", 'CS'):
        symsetStr = ','.join(cn_stk_indus())
    elif symset.find("CS_") >=0:
        (setype, induroot) = symset.split(r'_')
        symsetStr = ','.join([x for x in cn_stk_indus() if x.find(induroot)>=0])
    else:
        symsetStr = symset
    
    rc = sorted(symsetStr.split(r','))
    if plus_all:
        if symset in ['CS_ALL', 'CF_ALL']:
            rc.append(symset)
        if symset in ['CS']:
            rc.append('CS_ALL')
    return rc

if __name__ == '__main__':
    #(QDF, DataCfg) = config_qdf('b')
    qps_print(get_ssns('CF'))
    qps_print(get_ssns('CS'))
    qps_print(get_futs_roots(regen=True))
    allMkts = getSymset('CF_ALL')
    qps_print(allMkts, len(allMkts))


