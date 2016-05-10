import urllib
import urllib2
import xml.etree.ElementTree as et
from indra.literature import pubmed_client

pmc_url = 'http://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi'
pmid_convert_url = 'http://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/'


def id_lookup(paper_id, idtype=None):
    """This function takes a Pubmed ID, Pubmed Central ID, or DOI
    and use the Pubmed ID mapping
    service and looks up all other IDs from one
    of these. The IDs are returned in a dictionary."""
    if idtype is not None and idtype not in ('pmid', 'pmcid', 'doi'):
        raise ValueError("Invalid idtype %s; must be 'pmid', 'pmcid', "
                         "or 'doi'." % idtype)
    if paper_id.upper().startswith('PMC'):
        idtype = 'pmcid'
    # Strip off any prefix
    if paper_id.upper().startswith('PMID'):
        paper_id = paper_id[4:]
    elif paper_id.upper().startswith('DOI'):
        paper_id = paper_id[3:]
    data = {'ids': paper_id}
    if idtype is not None:
        data['idtype'] = idtype
    tree = pubmed_client.send_request(pmid_convert_url, urllib.urlencode(data))
    if tree is None:
        return {}
    record = tree.find('record')
    if record is None:
        return {}
    doi = record.attrib.get('doi')
    pmid = record.attrib.get('pmid')
    pmcid = record.attrib.get('pmcid')
    ids = {'doi': doi,
           'pmid': pmid,
           'pmcid': pmcid}
    return ids


def get_ids(search_term, retmax=1000):
    return pubmed_client.get_ids(search_term, retmax=retmax, db='pmc')


def get_xml(pmc_id):
    if pmc_id.upper().startswith('PMC'):
        pmc_id = pmc_id[3:]

    params = {}
    params['verb'] = 'GetRecord'
    params['identifier'] = 'oai:pubmedcentral.nih.gov:%s' % pmc_id
    params['metadataPrefix'] = 'pmc'

    data = urllib.urlencode(params)
    req = urllib2.Request(pmc_url, data)
    try:
        res = urllib2.urlopen(req)
    except urllib2.HTTPError:
        print 'Couldn\'t download PMC%d' % pmc_id
    xml_str = res.read()
    xml_str = xml_str.decode('utf-8')

    err = check_xml_error(xml_str)
    if err is None:
        return xml_str
    else:
        print 'PMC client returned with error %s: %s' % (err[0], err[1])
        return None


def check_xml_error(xml_str):
    tree = et.fromstring(xml_str.encode('utf-8'))
    xmlns = "http://www.openarchives.org/OAI/2.0/"
    err_tag = tree.find('{%s}error' % xmlns)
    if err_tag is not None:
        err_code = err_tag.attrib['code']
        err_text = err_tag.text
        return (err_code, err_text)
    return None
