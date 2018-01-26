import sys
import random
import tarfile
import pickle
from os import mkdir, path
from subprocess import call
from datetime import datetime
from indra.db.content_manager import PmcOA
from indra.tools.reading.read_files import read_files
from indra.tools.reading.readers import get_readers
from indra.tools.reading.script_tools import make_statements

this_dir = path.dirname(path.abspath(__file__))

def pdftotext(pdf_file_path, txt_file_path=None):
    '''Wrapper around the command line function of the same name'''
    if txt_file_path is None:
        txt_file_path = pdf_file_path.replace('.pdf', '.txt')
    elif callable(txt_file_path):
        txt_file_path = txt_file_path(pdf_file_path)

    call(['pdftotext', pdf_file_path, txt_file_path])
    assert path.exists(txt_file_path),\
        "A txt file was not created or name is unknown!"

    return txt_file_path


def extract(fpath, dirname, pmcid):
    with tarfile.open(fpath, 'r:gz') as tar:
        mems = tar.getmembers()
        xml_mem = [m for m in mems if m.name.endswith('.nxml')][0]
        pdf_mem = [m for m in mems if m.name == (xml_mem.name[:-4] + 'pdf')][0]
        fpaths = []
        for mem in [xml_mem, pdf_mem]:
            s = tar.extractfile(mem).read()
            fpath = path.join(dirname, pmcid + '.' + mem.name.split('.')[-1])
            with open(fpath, 'wb') as f:
                f.write(s)
            fpaths.append(fpath)
    return tuple(fpaths)


def read_and_process(fpath_list, input_type, readers, dirname):
    print("Reading %s." % input_type)
    readings = read_files(fpath_list, readers, verbose=True, log=True, failure_ok=True)
    print("Got %d readings." % len(readings))
    reading_out_path = path.join(dirname, 'readings_%s.pkl' % input_type)
    with open(reading_out_path, 'wb') as f:
        pickle.dump([reading.make_tuple() for reading in readings], f)
    print("Reading outputs stored in %s." % reading_out_path)

    stmt_data_list = make_statements(readings)
    stmts_pkl_path = path.join(dirname, 'stmts_%s.pkl' % input_type)
    with open(stmts_pkl_path, 'wb') as f:
        pickle.dump([sd.statement for sd in stmt_data_list], f)
        print("Statements pickled in %s." % stmts_pkl_path)
    return


def run_benchmark(num_samples, n_proc):
    pmc = PmcOA()
    print("Loading list of pdfs...")
    oa_pdf_list = pmc.ftp.get_csv_as_dict('oa_non_comm_use_pdf.csv', header=0)
    print("Loading list of all files...")
    oa_file_list = pmc.ftp.get_csv_as_dict('oa_file_list.csv', header=0)
    oa_file_dict = {line['Accession ID']: line['File'] for line in oa_file_list}
    print("Selecting %d samples." % num_samples)
    oa_sample_list = random.sample(oa_pdf_list, num_samples)
    dirname = path.join(this_dir, 'benchmark_%d' % datetime.now().timestamp())
    print("Creating directory: %s" % dirname)
    mkdir(dirname)
    group_list = []
    fpath_dict = {k: [] for k in ['xml', 'txt']}
    for oa_sample in oa_sample_list:
        pmcid = oa_sample['Accession ID']
        pmcid_dir = path.join(dirname, pmcid)
        mkdir(pmcid_dir)
        print("Loading sample with pmcid: %s." % pmcid)
        fpath = pmc.ftp.download_file(oa_file_dict[pmcid], pmcid_dir, pmcid)
        print("Saved to: %s" % fpath)
        xml_path, pdf_path = extract(fpath, pmcid_dir, pmcid)
        print("Converting pdf to text...")
        txt_path = pdftotext(pdf_path)
        group_list.append((xml_path, txt_path))
        fpath_dict['xml'].append(xml_path)
        fpath_dict['txt'].append(txt_path)

    print("Beginning reading...")
    readings_dir = path.join(dirname, 'reading_dir')
    readers = [reader_class(base_dir=readings_dir, n_proc=n_proc)
               for reader_class in get_readers()]
    for input_type in ['xml', 'txt']:
        read_and_process(fpath_dict[input_type], input_type, readers, dirname)
    return group_list


if __name__ == '__main__':
    num_samples = int(sys.argv[1])
    num_proc = int(sys.argv[2])
    run_benchmark(num_samples, num_proc)
