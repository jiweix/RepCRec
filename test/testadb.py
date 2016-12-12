import subprocess
import os


this_dir = os.path.dirname(__file__)
data_path = os.path.join(this_dir, 'data')

def test_data():
    filenames = os.listdir(data_path)
    for infile in filenames:
        if infile.endswith('.txt'):
            outfile = infile[:-4] + '.ans'
            if outfile in filenames:
                yield check_output, infile, outfile


def check_output(infile, outfile):
    proj_dir = os.path.join(this_dir, os.pardir)
    adb_path = os.path.join(proj_dir, 'src', 'adb.py')
    try:
        subprocess.check_call('python %s %s | diff -w - %s' % (
            adb_path, 
            os.path.join(data_path, infile), 
            os.path.join(data_path, outfile)), shell=True)
    except:
        assert False