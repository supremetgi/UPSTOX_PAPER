k = {}

k["something"] = 2

if "name" not in  k:
    k["name"] ="red"



h = k.get("t")
if h is None :
    print('nothing')



if "newrandom" not in k:
    k["newrandom"] = 2



print(k.get("newrandom"))