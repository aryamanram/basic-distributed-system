import sys
import socket
import string
import random

def randomline(characters=20):
    temp="".join(random.sample(string.ascii_uppercase, characters))
    temp+="\n"
    return temp
def knownline(klstring="ERROR", characters=20):
    index=random.randint(0,characters-len(klstring))
    if index==0:
        temp=klstring
        temp+="".join(random.sample(string.ascii_uppercase, characters-len(klstring)))
    elif index==20-len(klstring):
        temp="".join(random.sample(string.ascii_uppercase, characters-len(klstring)))
        temp+=klstring
    else:
        temp="".join(random.sample(string.ascii_uppercase, index))
        temp+=klstring
        temp+="".join(random.sample(string.ascii_uppercase, characters-(len(klstring)+index)))
    temp+="\n"
    return temp

if __name__ == '__main__':
    # First parameter is the number of lines in the logfile
    # Second parameter is the String which is present in the knownline
    hostname=socket.gethostname()
    filename=1
    result=""
    lines=int(sys.argv[1])
    klstring=(sys.argv[2] if len(sys.argv)>2 else "")
    for line in range(lines):
        choice=random.randint(0,1)
        if choice==0:
            result+=randomline()
        else:
            if klstring!="" and len(klstring)<30:
                result+=knownline(klstring)
            else:
                result+=knownline()
    # print(result)
    f= open(f"test10.txt","w")
    f.write(result)
    f.close()