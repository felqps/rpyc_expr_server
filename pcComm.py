import os
import shutil
import gzip
import zipfile
import pickle

def dummy():
    return

'''
f = file("test.zip")
z = zipfile.ZipFile(f)
NB: It's important that if you modify a zip file, you always close the archive. Even though we won’t be changing the file in this article, closing the archive is a good habit to be in. The following code will close a ZipFile:
z.close()
Now that we've got a ZipFile, what can we do with it? If all you need is to pull out the contents of the archive, then you can use the read method:

>>> print z.read("file1.txt")

for filename in sys.argv[1:]:
 z = zipfile.ZipFile(file(filename))
 print "%s:" % (filename)
 for f in sorted(z.namelist()):
 print "\t%s" % (f)
 print ""

file_name = "my_python_files.zip"
with ZipFile(file_name, 'r') as zip:
    zip.printdir()
    # extracting all the files
    print('Extracting all the files now...')
    zip.extractall()
    zip.extract('python_files/python_wiki.txt')
    print('Done!')
'''

#unzip .gz, .zip or .7z files
def unZipFile(fIn, dirOut):
    baseName = os.path.basename(fIn)
    print("unzip file:", fIn)
    #dirName = os.path.dirname(fIn) #no '/' at end
    if baseName.lower().endswith(".gz"):
        fOut = baseName[:-3] #drop ".gz"
        with gzip.open(fIn,"rb") as f_in, open(dirOut+fOut,"wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    elif baseName.lower().endswith(".zip") or baseName.lower().endswith(".z"):
        with zipfile.ZipFile(fIn, 'r') as zip_ref:
            zip_ref.extractall(dirOut)
    elif baseName.lower().endswith(".7z"):
        from pyunpack import Archive
        Archive(fIn).extractall(dirOut)
    return

def chkDfNullPct(df, dfName, thAll, thCol):
    nulPct = df.isna().sum().sum() / (df.index.size * df.columns.size)
    if nulPct > thAll:
        print(f"{dfName} all: nulPct={nulPct} > {thAll}")
    sparseCols = list()
    for i in df.columns:
        nulPct = df[i].isna().sum() / df.index.size
        if nulPct > thCol:
            sparseCols.append(i)
            #print(f"{dfName} col {i}: nulPct={nulPct} > {thCol}")
    return sparseCols
    
#clean up na in data file and write to csv
def cleanNa(inFile, outFile, naThAll, naThCol, fillna):
    bName = os.path.basename(inFile)
    df = pickle.load(open(inFile, "rb"))
    print("orig shape:", df.shape)
    spsCols = chkDfNullPct(df, bName, naThAll, naThCol)
    df.drop(columns=spsCols, inplace=True)
    print(f"{bName}: {len(spsCols)} cols with null% > {naThCol}, dropped")
    print("cleaned shape:", df.shape)
    if df.columns.size > 0:
        if fillna == 0:
            df.fillna(0, inplace=True)
        elif fillna == "ffill":
            df.fillna(method="ffill", axis=0, inplace=True)
    if outFile != "":
        df.to_csv(outFile, index=False)
    return df
    
    
