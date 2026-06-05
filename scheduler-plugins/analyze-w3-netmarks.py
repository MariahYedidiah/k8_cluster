import re, os, glob

base="results/W3-network"
scheds=["default","criticality","clustercriticality","clusterhybrid","hybridmerged"]
outdir=f"{base}/summary"
os.makedirs(outdir, exist_ok=True)

score_re=re.compile(r"pod=(\S+).*?node=(\S+).*?net_score=(\d+).*?final_score=(\d+)")

def shortpod(p):
    return "-".join(p.split("-")[:2]) if p.startswith("w3-") else p

rows=[]
for s in scheds:
    placement={}
    pfile=f"{base}/{s}/placement.txt"
    if os.path.exists(pfile):
        for line in open(pfile):
            parts=line.split()
            if len(parts)>=7 and parts[0].startswith("w3-") and parts[2]=="Running":
                placement[shortpod(parts[0])] = parts[6]

    scores={}
    lfile=f"{base}/{s}/scheduler.log"
    if os.path.exists(lfile):
        for line in open(lfile, errors="ignore"):
            m=score_re.search(line)
            if m:
                pod,node,net,final=m.groups()
                pod=shortpod(pod)
                scores.setdefault(pod,[]).append((node,int(net),int(final)))

    for pod, chosen in placement.items():
        cand=scores.get(pod,[])
        if not cand:
            rows.append([s,pod,chosen,"N/A","N/A","N/A","no custom score log"])
            continue
        maxnet=max(x[1] for x in cand)
        best=[x[0] for x in cand if x[1]==maxnet]
        chosen_net=next((x[1] for x in cand if x[0]==chosen), "unknown")
        ok="YES" if chosen in best else "NO"
        rows.append([s,pod,chosen,chosen_net,maxnet,",".join(best),ok])

with open(f"{outdir}/w3-netmarks-choice-analysis.tsv","w") as f:
    f.write("scheduler\tpod_group\tchosen_node\tchosen_net_score\tbest_net_score\tbest_net_nodes\tchosen_best_net\n")
    for r in rows:
        f.write("\t".join(map(str,r))+"\n")

with open(f"{outdir}/w3-netmarks-thesis-summary.txt","w") as f:
    for s in scheds:
        data=[r for r in rows if r[0]==s]
        valid=[r for r in data if r[-1] in ("YES","NO")]
        yes=sum(1 for r in valid if r[-1]=="YES")
        f.write(f"{s}: {yes}/{len(valid)} scored pod-groups selected a highest-net-score node\n")
