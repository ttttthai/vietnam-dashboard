import sys,subprocess,glob
from PIL import Image
pdf,page,tag=sys.argv[1],int(sys.argv[2]),sys.argv[3]
dpi=sys.argv[4] if len(sys.argv)>4 else '170'
subprocess.run(['pdftoppm','-r',dpi,'-png','-f',str(page),'-l',str(page),pdf,f'{tag}_full'])
f=sorted(glob.glob(f'{tag}_full*.png'))[-1]
im=Image.open(f); W,H=im.size
im.crop((0,int(H*0.03),W,int(H*0.52))).save(f'{tag}_a.png'); im.crop((0,int(H*0.48),W,int(H*0.97))).save(f'{tag}_b.png')
print(W,H)
