# from numpy import nan as NaN as nan
# from numpy import nan as NaN

from numpy import nan
from numpy import nan as NaN

class VpsObj():
    qtPrefix = "eb"
    omsPrefix = "test"
    def __init__(self):
        self._flds = {}
        self.brkr = "XYFT"
        self.client_ord_id = "bcAA0060"
    def msgType(self):
        return "VpsObj"
        
    def subject(self):
        return "%s.%s.%s.%s.%s"%(VpsObj.omsPrefix, "oms", self.msgType(), self.brkr, self.client_ord_id)

    def __str__(self):
        return(self.subject())

#    
#    def flds(self):
#        return self._flds
#    
#    def __getattr__(self, name):
#        if name == "_flds":
#            return self._flds
#        if name in self.flds().keys():
#            return self.flds()[name]
#        else:
#            raise AttributeError
#    
#    def __setattr__(self, name, value):
#        if name in self.flds().keys():
#            raise Exception(f"Cannot change value of {name}.")
#        self.flds()[name] = value

if __name__ == '__main__':
        ii = VpsObj()
        print("%s"%(ii))
