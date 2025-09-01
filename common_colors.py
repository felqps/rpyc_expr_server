
# Black        0;30     Dark Gray     1;30
# Red          0;31     Light Red     1;31
# Green        0;32     Light Green   1;32
# Brown/Orange 0;33     Yellow        1;33
# Blue         0;34     Light Blue    1;34
# Purple       0;35     Light Purple  1;35
# Cyan         0;36     Light Cyan    1;36
# Light Gray   0;37     White         1;37

CYAN='\033[0;36m'
BROWN='\033[0;33m'
DARKGRAY='\033[0;30m'
GRAY='\033[0;37m'
BLUE='\033[0;34m'
RED='\033[0;31m'
PURPLE='\033[0;35m'
GREEN='\033[0;32m'
DBG='\033[0;36m'
WRN='\033[0;33m'
ERR='\033[0;31m'
IMP='\033[0;36m'
NC='\033[0m' # No Color

STATUS_FOUND=f"{GREEN}FOUND{NC}"
STATUS_MISSING=f"{BLUE}MISSING{NC}"

def colored_boolean(b):
    if b:
        return f"{b}"
    else:
        return f"{RED}{b}{NC}"
