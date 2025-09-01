import os
from copy import deepcopy
from pathlib import Path

vpsCfg = {}
vpsCfg["dev"] = {
    "nats_qt": "nats://192.168.20.181:4222",
    "nats_oms": "nats://192.168.20.181:4222",
    "nats_ext": "nats://192.168.20.181:4222",
    "nats_graph_engine": "nats://192.168.20.181:4222",
    "nats_prefix": "eb",
    "regions": ["cn"],
    "tickTypes": ["blptick", "ltqtick", "VpsQuote", "VpsBar"],
    "secTypes": ["fut", "stk", "fx"],
    "qtSrcs": ["blp", "ltq"],
    "json_str_domains":["pgan"],
    "influx":{
        "ip": "192.168.20.181",
        "port": 8086,
        "username": "",
        "password": "",
        "database": "tradingDb",
    }
}


vpsCfg["dev-axiong"] = deepcopy(vpsCfg["dev"])
vpsCfg["dev-axiong"].update(
    {
        "nats_qt": "nats://192.168.20.214:4222",
        "nats_oms": "nats://192.168.20.214:4222",
        "nats_ext": "nats://192.168.20.214:4222",
        "nats_graph_engine": "nats://192.168.20.214:4222",
        "json_str_domains":["pgan"],
        "influx":{
            "ip": "192.168.20.214",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["axiong"] = deepcopy(vpsCfg["dev"])
vpsCfg["axiong"].update(
    {
        "nats_prefix": "axiong",
        "nats_qt": "nats://192.168.20.214:4222",
        "nats_oms": "nats://192.168.20.214:4222",
        "nats_ext": "nats://192.168.20.214:4222",
        "nats_graph_engine": "nats://192.168.20.214:4222",
        "json_str_domains":["pgan", "axiong"],
        "influx":{
            "ip": "192.168.20.214",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["axiong02"] = deepcopy(vpsCfg["dev"])
vpsCfg["axiong02"].update(
    {
        "nats_prefix": "axiong02",
        "nats_qt": "nats://192.168.20.214:4222",
        "nats_oms": "nats://192.168.20.214:4222",
        "nats_ext": "nats://192.168.20.214:4222",
        "nats_graph_engine": "nats://192.168.20.214:4222",
        "json_str_domains":["pgan"],
        "influx":{
            "ip": "192.168.20.214",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["axiong02_play_bin"] = deepcopy(vpsCfg["dev"])
vpsCfg["axiong02_play_bin"].update(
    {
        "nats_prefix": "axiong02_play_bin",
        "nats_qt": "nats://192.168.20.214:4222",
        "nats_oms": "nats://192.168.20.214:4222",
        "nats_ext": "nats://192.168.20.214:4222",
        "nats_graph_engine": "nats://192.168.20.214:4222",
        "json_str_domains":["pgan"],
        "influx":{
            "ip": "192.168.20.214",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["xyft"] = deepcopy(vpsCfg["dev"])
vpsCfg["xyft"].update(
    {
        "nats_prefix": "xyft",
        "nats_qt": "nats://192.168.20.185:4266",
        "nats_oms": "nats://192.168.20.185:4266",
        "nats_ext": "nats://192.168.20.185:4266",
        "nats_graph_engine": "nats://192.168.20.185:4266",
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["zxft"] = deepcopy(vpsCfg["dev"])
vpsCfg["zxft"].update(
    {
        "nats_prefix": "zxft",
        "nats_qt": "nats://192.168.20.185:4266",
        "nats_oms": "nats://192.168.20.185:4266",
        "nats_ext": "nats://192.168.20.185:4266",
        "nats_graph_engine": "nats://192.168.20.185:4266",
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["pgan_wan"] = deepcopy(vpsCfg["dev"])
vpsCfg["pgan_wan"].update(
    {
        "nats_prefix": "pgan",
        "nats_qt": "nats://192.168.20.46:4255",
        "nats_oms": "nats://192.168.20.46:4255",
        "nats_ext": "nats://192.168.20.46:4255",
        "nats_graph_engine": "nats://192.168.20.46:4255",
        "json_str_domains":["pgan"],
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        },
        "pulsar":{
            "wan":{
                "addr":"pulsar://119.145.105.20:36650,58.253.81.32:36650/",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB", 
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IifQ.MEUCIQCWS1PFN8UddDacTW+dM/pJ93J6bVgCYo1R5XPZBsD4RwIgVUMIBq07y9UQ1VoDIiKzUMf3lxkG3XN6V1WjVarE87M=",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB001",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IwMDEifQ.MEUCIQDLhPrn6faxWn/cD0I1YAAk3ObrbAa6yHi5mWOXz3htWAIgIKAymon3ukR9q5jCMubQ2Mmd+0CoiOxYqWm0KLg/Sm0=",
            },
            "wan1":{
                "addr":"pulsar://119.145.105.20:36650,58.253.81.32:36650/",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB1",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IxIn0.MEQCIGAY+beu7YJ1hczdp39OGCy0kyXZFXv+R+p/qihQU3T8AiAQBG/8nzB1idGsDFrj6jFu0/BjpZz1KqyYjqIL8WRlEg==",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB2",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IyIn0.MEUCIQDfFyL1IvXupX6vJQPksLkB5lJCYPFiITYi3pA7sJReYwIgYcBKMAOj+ConUwbeZuGAgVfGjQKDciRO8L0GuL2o2A4=",
            },
        }
    }
)

vpsCfg["pgan_lan"] = deepcopy(vpsCfg["dev"])
vpsCfg["pgan_lan"].update(
    {
        "nats_prefix": "pgan",
        "nats_qt": "nats://172.31.44.177:4255",
        "nats_oms": "nats://172.31.44.177:4255",
        "nats_ext": "nats://172.31.44.177:4255",
        "nats_graph_engine": "nats://172.31.44.177:4255",
        "json_str_domains":["pgan", "zxin"],
        "pulsar":{
            "lan":{
                "addr":"pulsar://172.31.17.30:6650,172.31.17.31:6650,172.31.17.32:6650/",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IifQ.MEUCIQCWS1PFN8UddDacTW+dM/pJ93J6bVgCYo1R5XPZBsD4RwIgVUMIBq07y9UQ1VoDIiKzUMf3lxkG3XN6V1WjVarE87M=",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB001",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IwMDEifQ.MEUCIQDLhPrn6faxWn/cD0I1YAAk3ObrbAa6yHi5mWOXz3htWAIgIKAymon3ukR9q5jCMubQ2Mmd+0CoiOxYqWm0KLg/Sm0=",
            },
            "lan1":{
                "addr":"pulsar://172.31.17.30:6650,172.31.17.31:6650,172.31.17.32:6650/",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB1",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IxIn0.MEQCIGAY+beu7YJ1hczdp39OGCy0kyXZFXv+R+p/qihQU3T8AiAQBG/8nzB1idGsDFrj6jFu0/BjpZz1KqyYjqIL8WRlEg==",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB2",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IyIn0.MEUCIQDfFyL1IvXupX6vJQPksLkB5lJCYPFiITYi3pA7sJReYwIgYcBKMAOj+ConUwbeZuGAgVfGjQKDciRO8L0GuL2o2A4=",
            }
        }
    }
)


vpsCfg["pgan_new_wan"] = deepcopy(vpsCfg["dev"])
vpsCfg["pgan_new_wan"].update(
    {
        "nats_prefix": "pgan_new",
        "nats_qt": "nats://192.168.20.46:4277",
        "nats_oms": "nats://192.168.20.46:4277",
        "nats_ext": "nats://192.168.20.46:4277",
        "nats_graph_engine": "nats://192.168.20.46:4277",
        "json_str_domains":["pgan", "pgan_new"],
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        },
        "pulsar":{
            "wan":{
                "addr":"pulsar://119.145.105.20:36650,58.253.81.32:36650/",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB3", 
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IzIn0.MEQCIGK+7lssnuXGdUAVMoKL4NBt26GweIcJU5mWcwrltNeuAiBcdM0fDTffhJQVtIA96nvtWrdFIz1f4XOKCippJ1Ku6g==",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB4",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0I0In0.MEUCICB64GNGBUILLkCjxy+iad84+e/q1HT/TNewuPyI5LD0AiEA0zJx2OEbyCagEmu1g7OvAijnerirm9SwbtpuWSUZpFU=",
            },
            "wan1":{
                "addr":"pulsar://119.145.105.20:36650,58.253.81.32:36650/",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB5",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0I1In0.MEUCIQDvHU+/7Pm984lNz2ZVeqqz+IVTuOjbpHa5nlwS0zofLAIgJscgWRffGufEv3IULQBsKH4xpmb8HKhlyDX0AonVbnU=",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB6",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0I2In0.MEUCIQDvSuekEDf+lm3R9NjdaDiiSjbH9byBeVbZp/fRhciQBAIgSzAq2Lds04cPc0r6pyhHSWG5fppysVyd/gvFJq9O2yU=",
            },
        }
    }
)

vpsCfg["pgan_zq500_wan"] = deepcopy(vpsCfg["dev"])
vpsCfg["pgan_zq500_wan"].update(
    {
        "nats_prefix": "pgan_zq500",
        "nats_qt": "nats://192.168.20.46:4277",
        "nats_oms": "nats://192.168.20.46:4277",
        "nats_ext": "nats://192.168.20.46:4277",
        "nats_graph_engine": "nats://192.168.20.46:4277",
        "json_str_domains":["pgan", "pgan_new"],
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        },
        "pulsar":{
            "wan":{
                "addr":"pulsar://119.145.105.20:36650,58.253.81.32:36650/",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB3", 
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IzIn0.MEQCIGK+7lssnuXGdUAVMoKL4NBt26GweIcJU5mWcwrltNeuAiBcdM0fDTffhJQVtIA96nvtWrdFIz1f4XOKCippJ1Ku6g==",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB4",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0I0In0.MEUCICB64GNGBUILLkCjxy+iad84+e/q1HT/TNewuPyI5LD0AiEA0zJx2OEbyCagEmu1g7OvAijnerirm9SwbtpuWSUZpFU=",
            },
            "wan1":{
                "addr":"pulsar://119.145.105.20:36650,58.253.81.32:36650/",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB5",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0I1In0.MEUCIQDvHU+/7Pm984lNz2ZVeqqz+IVTuOjbpHa5nlwS0zofLAIgJscgWRffGufEv3IULQBsKH4xpmb8HKhlyDX0AonVbnU=",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB6",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0I2In0.MEUCIQDvSuekEDf+lm3R9NjdaDiiSjbH9byBeVbZp/fRhciQBAIgSzAq2Lds04cPc0r6pyhHSWG5fppysVyd/gvFJq9O2yU=",
            },
        }
    }
)

vpsCfg["pgan_new_lan"] = deepcopy(vpsCfg["dev"])
vpsCfg["pgan_new_lan"].update(
    {
        "nats_prefix": "pgan_new",
        "nats_qt": "nats://26.24.0.11:4255",
        "nats_oms": "nats://26.24.0.11:4255",
        "nats_ext": "nats://26.24.0.11:4255",
        "nats_graph_engine": "nats://26.24.0.11:4255",
        "json_str_domains":["pgan", "pgan_new", "pgan_zq500"],
        "pulsar":{
            "lan":{
                "addr":"pulsar://172.31.17.30:6650,172.31.17.31:6650,172.31.17.32:6650/",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB3",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IzIn0.MEQCIGK+7lssnuXGdUAVMoKL4NBt26GweIcJU5mWcwrltNeuAiBcdM0fDTffhJQVtIA96nvtWrdFIz1f4XOKCippJ1Ku6g==",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB4",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0I0In0.MEUCICB64GNGBUILLkCjxy+iad84+e/q1HT/TNewuPyI5LD0AiEA0zJx2OEbyCagEmu1g7OvAijnerirm9SwbtpuWSUZpFU=",
            },
            "lan1":{
                "addr":"pulsar://172.31.17.30:6650,172.31.17.31:6650,172.31.17.32:6650/",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB5",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0I1In0.MEUCIQDvHU+/7Pm984lNz2ZVeqqz+IVTuOjbpHa5nlwS0zofLAIgJscgWRffGufEv3IULQBsKH4xpmb8HKhlyDX0AonVbnU=",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB6",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0I2In0.MEUCIQDvSuekEDf+lm3R9NjdaDiiSjbH9byBeVbZp/fRhciQBAIgSzAq2Lds04cPc0r6pyhHSWG5fppysVyd/gvFJq9O2yU=",
            }
        }
    }
)


vpsCfg["simu_pgan_wan"] = deepcopy(vpsCfg["dev"])
vpsCfg["simu_pgan_wan"].update(
    {
        "nats_prefix": "pgan",
        "nats_qt": "nats://192.168.20.214:4222",
        "nats_oms": "nats://192.168.20.214:4222",
        "nats_ext": "nats://192.168.20.214:4222",
        "nats_graph_engine": "nats://192.168.20.214:4222",
        "json_str_domains":["pgan"],
        "influx":{
            "ip": "192.168.20.214",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        },
        "pulsar":{
            "wan":{
                "addr":"pulsar://119.145.105.20:36650,58.253.81.32:36650/",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IifQ.MEUCIQCWS1PFN8UddDacTW+dM/pJ93J6bVgCYo1R5XPZBsD4RwIgVUMIBq07y9UQ1VoDIiKzUMf3lxkG3XN6V1WjVarE87M=",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB001",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IwMDEifQ.MEUCIQDLhPrn6faxWn/cD0I1YAAk3ObrbAa6yHi5mWOXz3htWAIgIKAymon3ukR9q5jCMubQ2Mmd+0CoiOxYqWm0KLg/Sm0=",
            },
            "wan1":{
                "addr":"pulsar://119.145.105.20:36650,58.253.81.32:36650/",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB1",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IxIn0.MEQCIGAY+beu7YJ1hczdp39OGCy0kyXZFXv+R+p/qihQU3T8AiAQBG/8nzB1idGsDFrj6jFu0/BjpZz1KqyYjqIL8WRlEg==",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB2",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IyIn0.MEUCIQDfFyL1IvXupX6vJQPksLkB5lJCYPFiITYi3pA7sJReYwIgYcBKMAOj+ConUwbeZuGAgVfGjQKDciRO8L0GuL2o2A4=",
            },
        }
    }
)

vpsCfg["simu_pgan_lan"] = deepcopy(vpsCfg["dev"])
vpsCfg["simu_pgan_lan"].update(
    {
        "nats_prefix": "pgan",
        "nats_qt": "nats://192.168.20.214:4223",
        "nats_oms": "nats://192.168.20.214:4223",
        "nats_ext": "nats://192.168.20.214:4223",
        "nats_graph_engine": "nats://192.168.20.214:4223",
        "json_str_domains":["pgan", "zxin"],
        "pulsar":{
            "lan":{
                # "addr":"pulsar://172.31.17.30:6650,172.31.17.31:6650,172.31.17.32:6650/",
                "addr":"pulsar://119.145.105.20:36650,58.253.81.32:36650/",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IifQ.MEUCIQCWS1PFN8UddDacTW+dM/pJ93J6bVgCYo1R5XPZBsD4RwIgVUMIBq07y9UQ1VoDIiKzUMf3lxkG3XN6V1WjVarE87M=",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB001",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IwMDEifQ.MEUCIQDLhPrn6faxWn/cD0I1YAAk3ObrbAa6yHi5mWOXz3htWAIgIKAymon3ukR9q5jCMubQ2Mmd+0CoiOxYqWm0KLg/Sm0=",
            },
            "lan1":{
                "addr":"pulsar://119.145.105.20:36650,58.253.81.32:36650/",
                # "addr":"pulsar://172.31.17.30:6650,172.31.17.31:6650,172.31.17.32:6650/",
                "producer_topic":"persistent://sbtps-qpp/qpp/PAISSB1",
                "producer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IxIn0.MEQCIGAY+beu7YJ1hczdp39OGCy0kyXZFXv+R+p/qihQU3T8AiAQBG/8nzB1idGsDFrj6jFu0/BjpZz1KqyYjqIL8WRlEg==",
                "consumer_topic":"persistent://sbtps-qpp/qpp/PAISSB2",
                "consumer_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJQQUlTU0IyIn0.MEUCIQDfFyL1IvXupX6vJQPksLkB5lJCYPFiITYi3pA7sJReYwIgYcBKMAOj+ConUwbeZuGAgVfGjQKDciRO8L0GuL2o2A4=",
            }
        }
    }
)

vpsCfg["papr"] = deepcopy(vpsCfg["dev"])
vpsCfg["papr"].update(
    {
        "nats_prefix": "papr",
        "nats_qt": "nats://192.168.20.46:4288",
        "nats_oms": "nats://192.168.20.46:4288",
        "nats_ext": "nats://192.168.20.46:4288",
        "nats_graph_engine": "nats://192.168.20.46:4288",
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["papr_fut"] = deepcopy(vpsCfg["dev"])
vpsCfg["papr_fut"].update(
    {
        "nats_prefix": "papr_fut",
        "nats_qt": "nats://192.168.20.185:4288",
        "nats_oms": "nats://192.168.20.185:4288",
        "nats_ext": "nats://192.168.20.185:4288",
        "nats_graph_engine": "nats://192.168.20.185:4288",
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["zxin"] = deepcopy(vpsCfg["dev"])
vpsCfg["zxin"].update(
    {
        "nats_prefix": "zxin",
        "nats_qt": "nats://192.168.20.46:4255",
        "nats_oms": "nats://192.168.20.46:4255",
        "nats_ext": "nats://192.168.20.46:4255",
        "nats_graph_engine": "nats://192.168.20.46:4255",
        # "json_str_domains":["pgan", "zxin"],
        "json_str_domains":["pgan"],
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["swan"] = deepcopy(vpsCfg["dev"])
vpsCfg["swan"].update(
    {
        "nats_prefix": "swan",
        "nats_qt": "nats://192.168.20.46:4255",
        "nats_oms": "nats://192.168.20.46:4255",
        "nats_ext": "nats://192.168.20.46:4255",
        "nats_graph_engine": "nats://192.168.20.46:4255",
        # "json_str_domains":["pgan", "zxin"],
        "json_str_domains":["pgan"],
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["public_network_wan"] = deepcopy(vpsCfg["dev"])
vpsCfg["public_network_wan"].update(
    {
        "nats_prefix": "public_network_wan",
        "nats_qt": "nats://58.246.73.60:4211",
        "nats_oms": "nats://58.246.73.60:4211",
        "nats_ext": "nats://58.246.73.60:4211",
        "nats_graph_engine": "nats://58.246.73.60:4211",
        "json_str_domains":["pgan"],
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["public_network_lan"] = deepcopy(vpsCfg["dev"])
vpsCfg["public_network_lan"].update(
    {
        "nats_prefix": "public_network_lan",
        "nats_qt": "nats://192.168.20.53:4211",
        "nats_oms": "nats://192.168.20.53:4211",
        "nats_ext": "nats://192.168.20.53:4211",
        "nats_graph_engine": "nats://192.168.20.53:4211",
        "json_str_domains":["pgan"],
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["simu_zxin"] = deepcopy(vpsCfg["dev"])
vpsCfg["simu_zxin"].update(
    {
        "nats_prefix": "zxin",
        "nats_qt": "nats://192.168.20.214:4222",
        "nats_oms": "nats://192.168.20.214:4222",
        "nats_ext": "nats://192.168.20.214:4222",
        "nats_graph_engine": "nats://192.168.20.214:4222",
        # "json_str_domains":["pgan", "zxin"],
        "json_str_domains":["pgan"],
        "influx":{
            "ip": "192.168.20.214",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

vpsCfg["qpscloud"] = deepcopy(vpsCfg["dev"])
vpsCfg["qpscloud"].update(
    {
        "nats_prefix": "qpscloud",
        "nats_qt": "nats://192.168.20.46:4288",
        "nats_oms": "nats://192.168.20.46:4288",
        "nats_ext": "nats://192.168.20.46:4288",
        "nats_graph_engine": "nats://192.168.20.46:4288",
        "influx":{
            "ip": "192.168.20.53",
            "port": 8086,
            "username": "",
            "password": "",
            "database": "tradingDb",
        }
    }
)

class VpsCfg:
    def __init__(self, nm="dev"):
        fn_cfg = "/qpsdata/config/VpsCfg.cfg"
        if os.path.exists(fn_cfg):
            extra_cfg = eval(Path(fn_cfg).read_text())
            for k,v in extra_cfg.items():
                vpsCfg[k] = deepcopy(vpsCfg["dev"])
                vpsCfg[k].update(v)
            print(f"Load {fn_cfg} success")

        self._cfg = vpsCfg[nm]
        for tickType in self.tickTypes:
            for secType in self.secTypes:
                for region in self.regions:
                    self._cfg[
                        "subject_qt_{}_{}_{}".format(tickType, secType, region)
                    ] = "{}.qt.{}.{}.{}.>".format(self.prefix(), tickType, secType, region)

    def prefix(self):
        return self._cfg["nats_prefix"]

    def cfg(self):
        return self._cfg

    def print(self):
        for k in sorted(self._cfg.keys()):
            print("{}: {}".format(k, self.cfg()[k]))

    def __getattr__(self, name):
        if name.find("__") < 0:  # Use '__' to do pattern matches
            return self._cfg[name]
        else:
            res = []
            for k in sorted(self.cfg().keys()):
                if k.find(name.replace("__", "")) >= 0:
                    res.append(self.cfg()[k])
            return res


if __name__ == "__main__":
    cfg = VpsCfg("xyft")
    cfg.print()
    # cfg = VpsCfg('dev-cli')
    # cfg.print()
    # print(cfg.nats_prefix)
    # print(cfg.subject_qt_ltqtick_stk_cn)
    # print(cfg.__ltq__)
