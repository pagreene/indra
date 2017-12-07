from __future__ import absolute_import, print_function, unicode_literals
from builtins import dict, str
import re
import shutil
import gzip
import pickle
from indra.literature import id_lookup, pubmed_client
from datetime import datetime
#try:
import xml.etree.ElementTree as ET
#except:
#    import xml.etree as ET
from indra.util import UnicodeXMLTreeBuilder as UTB
from os import path, walk, remove
from subprocess import call
from collections import namedtuple
from indra.db import get_primary_db, texttypes, formats
from indra.util import zip_string

try:
    basestring
except:
    basestring = str

RE_PATT_TYPE = type(re.compile(''))

# TODO: This might do better in util, or somewhere more gnereal ===============
def deep_find(top_dir, patt, since_date=None, verbose=False):
    '''Find files that match `patt` recursively down from `top_dir`

    Note: patt may be a regex string or a regex pattern object.
    '''
    if not isinstance(patt, RE_PATT_TYPE):
        patt = re.compile(patt)

    def is_after_time(time_stamp, path_str):
        mod_time = datetime.fromtimestamp(path.getmtime(path_str))
        if mod_time < time_stamp:
            return False
        return True

    def desired_files(fname):
        if since_date is not None:
            if not is_after_time(since_date, fname):
                return False
        return patt.match(path.basename(fname)) is not None

    def desired_dirs(dirpath):
        if since_date is not None:
            if not is_after_time(since_date, dirpath):
                return False
        return True

    def complete(root, leaves):
        return [path.join(root, leaf) for leaf in leaves]

    matches = []
    try:
        for root, dirnames, filenames in walk(top_dir):
            if verbose:
                print("Looking in %s." % root)
            # Check if the directory has been modified recently. Note that removing
            # a dirpath from dirnames will prevent walk from going into that dir.
            if since_date is not None:
                for dirpath in filter(desired_dirs, complete(root, dirnames)):
                    dirnames.remove(dirpath)
            for filepath in filter(desired_files, complete(root, filenames)):
                matches.append(filepath)
    except:  # We want this to catch literally everything.
        print("Caught exception. Returning what we have so far.")

    return matches


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
#==============================================================================


def get_xml_data(pdf_path, entry_dict):
    'Get the data from the xml file if present'
    pdf_name = path.basename(pdf_path)
    art_dirname = path.abspath(pdf_path + '/'.join(4*['..']))
    xml_path_list = deep_find(art_dirname, pdf_name.replace('.pdf','\.xml.*?'))
    assert len(xml_path_list) > 0, "We have no metadata"
    if len(xml_path_list) == 1:
        xml = ET.parse(xml_path_list[0], parser=UTB())
    elif len(xml_path_list) > 1:
        # TODO: we really should be more intelligent about this
        xml = ET.parse(xml_path_list[0], parser=UTB())

    # Maybe include the journal subtitle too, in future.
    xml_data = {}
    for purpose_key, xml_label_dict in entry_dict.items():
        xml_data[purpose_key] = {}
        for table_key, xml_label in xml_label_dict.items():
            value = xml.find('.//' + xml_label)
            if not isinstance(value, basestring):
                value = value.text
            xml_data[purpose_key][table_key] = value.encode('utf-8')

    return xml_data


def find_other_ids(doi):
    '''Use the doi to try and find the pmid and/or pmcid.'''
    other_ids = dict(zip(['pmid', 'pmcid'], 2*[None]))
    id_dict = id_lookup(doi, 'doi')
    if id_dict['pmid'] is None:
        result_list = pubmed_client.get_ids(doi)
        for res in result_list:
            if 'PMC' in res:
                other_ids['pmcid'] = res
                # This is not very efficient...
                other_ids['pmid'] = id_lookup(res, 'pmcid')['pmid']
                break
        else:
            # This is based on a circumstantial assumption.
            # It will work for the test set, but may fail when
            # upon generalization.
            if len(result_list) == 1:
                other_ids['pmid'] = result_list[0]
            else:
                other_ids['pmid'] = None
    else:
        other_ids['pmid'] = id_dict['pmid']
        other_ids['pmcid'] = id_dict['pmcid']

    return other_ids


def process_one_pdf(pdf_path, txt_path, do_zip=True):
    'Convert the pdf to txt and zip it'
    txt_path = pdftotext(pdf_path, txt_path)
    with open(txt_path, 'rb') as f_in:
        with gzip.open(txt_path + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    with open(txt_path, 'rb') as f:
        if do_zip:
            content = zip_string(f.read().decode('utf-8'))
        else:
            content = f.read().decode('utf-8')

    remove(txt_path)  # Only a tmp file.
    return content


def zip_abstract(abst_el, ttl_el):
    'Get the abstract from the xml'
    abst_text = ET.tostring(
        abst_el,
        encoding='utf8',
        method='text'
        ).decode('utf8')
    return zip_string(abst_text + ttl_el.text)


def pdf_is_worth_uploading(ref_data):
    '''Determines if the data in the pdf is likely to be useful.

    Currently we are simply looking at the pmid and pmcid to see if either is
    present, however in future we should really implement a more meaningful
    method of determination.

    Returns: Bool
    '''
    return ref_data['pmid'] is not None or ref_data['pmcid'] is not None


def upload_springer(springer_dir, verbose=False, since_date=None,
                    batch_size=1000, db=None):
    '''Convert the pdfs to text and upload data to AWS

    Note: Currently does nothing.
    '''
    txt_path = 'tmp.txt'
    uploaded = []
    if verbose:
        vprint = print
    else:
        vprint = lambda x: None
    vprint("Looking for PDF`s.")
    match_list = deep_find(
        springer_dir, 
        '.*?\.pdf', 
        verbose=verbose,
        since_date=since_date
        )
    # TODO: cache the result of the search
    if db is None:
        db = get_primary_db()
    db.grab_session()
    copy_cols = ('text_ref_id', 'source', 'format', 'text_type', 'content')

    added = []
    vprint("Found %d PDF`s. Now entering loop." % len(match_list))
    content_list = []
    for i, pdf_path in enumerate(match_list):
        vprint("Examining %s" % pdf_path)
        xml_data = get_xml_data(
            pdf_path,
            entry_dict={
                'ref_data': {
                    'doi': 'ArticleDOI'
                    },
                'abst_data': {
                    'journal': 'JournalTitle',
                    'pub_date': 'Year',
                    'publisher': 'PublisherName',
                    'abstract': 'Abstract',
                    'title': 'ArticleTitle'
                    }
                }
            )
        ref_data = xml_data['ref_data']
        if not isinstance(ref_data['doi'], str):
            print('Need to decode ref_data doi.')
            ref_data['doi'] = ref_data['doi'].decode('utf8')
        ref_data.update(find_other_ids(ref_data['doi']))

        if not pdf_is_worth_uploading(ref_data):
            vprint("Skipping...")
            continue
        vprint("Processing...")

        # For now pmid's are the primary ID, so that should be the primary
        tr_list = db.select_all('text_ref', db.TextRef.pmid == ref_data['pmid'])
        suf = " text ref %%s for pmid: %s, and doi: %s." % (ref_data['pmid'], ref_data['doi'])
        if len(tr_list) is 0:
            text_ref_id = db.insert('text_ref', **ref_data)
            vprint("Inserted new" + suf % text_ref_id)
        else:
            text_ref_id = tr_list[0].id
            vprint("Found existing" + suf % text_ref_id)

        full_content = process_one_pdf(pdf_path, txt_path, do_zip=True)
        content_list.append((text_ref_id, 'Springer', formats.TEXT, 
                             texttypes.FULLTEXT, full_content.encode('utf8')))

        abst_data = xml_data['abst_data']
        if abst_data['abstract'] is not None:
            abst_content = zip_abstract(abst_data['abstract'], abst_data['title'])
            # TODO: Check if the abstract is already there.
            if len(tr_list) is 0:
                content_list.append((text_ref_id, 'Springer', formats.ABSTRACT,
                                     texttypes.ABSTRACT, abst_content))

        added.append({'path': pdf_path, 'doi': ref_data['doi']})
        if (i+1) % batch_size is 0:
            print('Uploading batch %d/%d' % (i+1, len(match_list)//batch_size))
            db.copy('text_content', content_list, copy_cols)
            uploaded += added
            content_list = []
            added = []

        vprint("Finished Processing...")

    if len(content_list) > 0:
        print('Uploading the %d remaining content.' % len(content_list))
        db.copy('text_content', content_list, copy_cols)
        uploaded += added
    return uploaded


if __name__ == "__main__":
    # TODO: we should probably support reading from a different
    # directory.
    record_file = path.join(path.dirname(path.abspath(__file__)), 'last_update.txt')
    if path.exists(record_file):
        with open(record_file, 'r') as f:
            last_update = datetime.fromtimestamp(float(f.read()))
    else:
        last_update = None

    default_dir = '/n/groups/lsp/darpa/springer/content/data'
    uploaded = upload_springer(default_dir, verbose=True, since_date=last_update)
    print("Uploaded %d papers." % len(uploaded))

    tstamp = datetime.now().timestamp()
    print("Completing with timestamp: %f" % tstamp)
    with open(record_file, 'w') as f:
        f.write(str(datetime.now().timestamp()))
