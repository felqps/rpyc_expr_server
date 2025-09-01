
#!/usr/bin/env python
# 

import yaml

def yaml_dump(opt, title="NA"):
    # print("set_opt_defaults", opt)
    print(f"INFO: {title} ... ")
    print(yaml.dump(opt))
