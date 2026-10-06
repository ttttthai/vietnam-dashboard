import sys,glob,subprocess,os
from PIL import Image
pdf,tag=sys.argv[1],sys.argv[2]
os.makedirs(tag,exist_ok=True)
subprocess.run(['pdftoppm','-r','60','-png',pdf,f'{tag}/p'])
fs=sorted(glob.glob(f'{tag}/p-*.png'))
ims=[Image.open(f).convert('L') for f in fs]
w=200;cols=6
th=[im.resize((w,int(im.height*w/im.width))) for im in ims]
h=max(t.height for t in th)
rows=(len(th)+cols-1)//cols
M=Image.new('L',(cols*w,rows*h),255)
for i,t in enumerate(th): M.paste(t,((i%cols)*w,(i//cols)*h))
M.save(f'{tag}_montage.png'); print(len(fs))
