# encoding: utf-8

import importlib.util
spec = importlib.util.spec_from_file_location("xml2kvp", "./core/xml2kvp.py")
xml2kvp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(xml2kvp)
import json
import difflib
import pprint

def test_xml():
    return '''<?xml version='1.0' encoding='UTF-8'?>
<oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dpla="http://dp.la/about/map/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/" xmlns:schema="http://schema.org">
  <dcterms:title>Boğazkale (Boğazköy, Hattusha), Turkey</dcterms:title>
  <dcterms:creator>Mellink, Machteld J. (Machteld Johanna)</dcterms:creator>
  <dcterms:description>Lion Gate. South side</dcterms:description>
  <dcterms:date>1953</dcterms:date>
  <dcterms:subject>14th-13th century BC</dcterms:subject>
  <dcterms:format>35mm Kodachrome slide</dcterms:format>
  <dcterms:type>Image</dcterms:type>
  <dcterms:rights>The images included in this collection are licensed under a Creative Commons Attribution-Noncommercial 3.0 United States License http://creativecommons.org/licenses/by-nc/3.0/us/</dcterms:rights>
  <dcterms:identifier>dplapa:BRYNMAWR_Mellink_3213</dcterms:identifier>
  <edm:isShownAt>http://triptych.brynmawr.edu/cdm/ref/collection/Mellink/id/3213</edm:isShownAt>
  <edm:preview>http://triptych.brynmawr.edu/utils/getthumbnail/collection/Mellink/id/3213</edm:preview>
  <dcterms:isPartOf>Machteld J. Mellink Collection of Archaeological Site Photography</dcterms:isPartOf>
  <edm:dataProvider>Bryn Mawr College</edm:dataProvider>
  <edm:provider>PA Digital</edm:provider>
</oai_dc:dc>
'''

def test_kvp():
    return '''{
    "oai_dc:dc|dcterms:title": "Bo\u011fazkale (Bo\u011fazk\u00f6y, Hattusha), Turkey",
    "oai_dc:dc|dcterms:creator": "Mellink, Machteld J. (Machteld Johanna)",
    "oai_dc:dc|dcterms:description": "Lion Gate. South side",
    "oai_dc:dc|dcterms:date": "1953",
    "oai_dc:dc|dcterms:subject": "14th-13th century BC",
    "oai_dc:dc|dcterms:format": "35mm Kodachrome slide",
    "oai_dc:dc|dcterms:type": "Image",
    "oai_dc:dc|dcterms:rights": "The images included in this collection are licensed under a Creative Commons Attribution-Noncommercial 3.0 United States License http://creativecommons.org/licenses/by-nc/3.0/us/",
    "oai_dc:dc|dcterms:identifier": "dplapa:BRYNMAWR_Mellink_3213",
    "oai_dc:dc|edm:isShownAt": "http://triptych.brynmawr.edu/cdm/ref/collection/Mellink/id/3213",
    "oai_dc:dc|edm:preview": "http://triptych.brynmawr.edu/utils/getthumbnail/collection/Mellink/id/3213",
    "oai_dc:dc|dcterms:isPartOf": "Machteld J. Mellink Collection of Archaeological Site Photography",
    "oai_dc:dc|edm:dataProvider": "Bryn Mawr College",
    "oai_dc:dc|edm:provider": "PA Digital"
}'''

def test_kvp_from_csv():
    return '''{
"dcterms:title": "Bo\u011fazkale (Bo\u011fazk\u00f6y, Hattusha), Turkey",
"dcterms:creator": "Mellink, Machteld J. (Machteld Johanna)",
"dcterms:description": "Lion Gate. South side",
"dcterms:date": "1953",
"dcterms:subject": "14th-13th century BC",
"dcterms:format": "35mm Kodachrome slide",
"dcterms:type": "Image",
"dcterms:rights": "The images included in this collection are licensed under a Creative Commons Attribution-Noncommercial 3.0 United States License http://creativecommons.org/licenses/by-nc/3.0/us/",
"dcterms:identifier": "dplapa:BRYNMAWR_Mellink_3213",
"edm:isShownAt": "http://triptych.brynmawr.edu/cdm/ref/collection/Mellink/id/3213",
"edm:preview": "http://triptych.brynmawr.edu/utils/getthumbnail/collection/Mellink/id/3213",
"dcterms:isPartOf": "Machteld J. Mellink Collection of Archaeological Site Photography",
"edm:dataProvider": "Bryn Mawr College",
"edm:provider": "PA Digital"
}
'''

def test_xml_config():
    return {
        "add_literals":{},
        "capture_attribute_values":[],
        "concat_values_on_all_fields":False,
        "concat_values_on_fields":{},
        "copy_to":{},
        "copy_to_regex":{},
        "copy_value_to_regex":{},
        "error_on_delims_collision":False,
        "exclude_attributes":[],
        "exclude_elements":[],
        "include_all_attributes":True,
        "include_attributes":[],
        "include_sibling_id":False,
        "multivalue_delim":"|",
        "node_delim":"|",
        "ns_prefix_delim":":",
        "remove_copied_key":True,
        "remove_copied_value":False,
        "remove_ns_prefix":False,
        "repeating_element_suffix_count":False,
        "self_describing":False,
        "skip_attribute_ns_declarations":True,
        "skip_repeating_values":True,
        "skip_root":False,
        "split_values_on_all_fields":None,
        "split_values_on_fields":{},
        "nsmap": {
            "dc":"http://purl.org/dc/elements/1.1/",
            "dcterms":"http://purl.org/dc/terms/",
            "edm":"http://www.europeana.eu/schemas/edm/",
            "oai_dc":"http://www.openarchives.org/OAI/2.0/oai_dc/",
            "dpla":"http://dp.la/about/map/",
            "schema":"http://schema.org",
            "oai_qdc":"http://worldcat.org/xmlschemas/qdc-1.0/"
        }
    }

def test_kvp_config():
    return {
        "add_literals":{},
        "capture_attribute_values":[],
        "concat_values_on_all_fields":False,
        "concat_values_on_fields":{},
        "copy_to":{},
        "copy_to_regex":{},
        "copy_value_to_regex":{},
        "error_on_delims_collision":False,
        "exclude_attributes":[],
        "exclude_elements":[],
        "include_all_attributes":True,
        "include_attributes":[],
        "include_sibling_id":False,
        "multivalue_delim":"|",
        "node_delim":"|",
        "ns_prefix_delim":":",
        "remove_copied_key":True,
        "remove_copied_value":False,
        "remove_ns_prefix":False,
        "repeating_element_suffix_count":False,
        "self_describing":False,
        "skip_attribute_ns_declarations":True,
        "skip_repeating_values":True,
        "skip_root":False,
        "split_values_on_all_fields":"|",
        "split_values_on_fields":{},
        "nsmap": {
            "dc":"http://purl.org/dc/elements/1.1/",
            "dcterms":"http://purl.org/dc/terms/",
            "edm":"http://www.europeana.eu/schemas/edm/",
            "oai_dc":"http://www.openarchives.org/OAI/2.0/oai_dc/",
            "dpla":"http://dp.la/about/map/",
            "schema":"http://schema.org",
            "oai_qdc":"http://worldcat.org/xmlschemas/qdc-1.0/"
        }
    }

def test_csv_config():
    return {
        "add_literals":{},
        "capture_attribute_values":[],
        "concat_values_on_all_fields":False,
        "concat_values_on_fields":{},
        "copy_to":{},
        "copy_to_regex":{},
        "copy_value_to_regex":{},
        "error_on_delims_collision":False,
        "exclude_attributes":[],
        "exclude_elements":[],
        "include_all_attributes":True,
        "include_attributes":[],
        "include_sibling_id":False,
        "multivalue_delim":"|",
        "node_delim":"|",
        "ns_prefix_delim":":",
        "remove_copied_key":True,
        "remove_copied_value":False,
        "remove_ns_prefix":False,
        "repeating_element_suffix_count":False,
        "self_describing":False,
        "skip_attribute_ns_declarations":True,
        "skip_repeating_values":True,
        "skip_root":False,
        "split_values_on_all_fields":"|",
        "split_values_on_fields":{},
        "add_element_root": "oai_dc:dc",
        "nsmap": {
            "dc":"http://purl.org/dc/elements/1.1/",
            "dcterms":"http://purl.org/dc/terms/",
            "edm":"http://www.europeana.eu/schemas/edm/",
            "oai_dc":"http://www.openarchives.org/OAI/2.0/oai_dc/",
            "dpla":"http://dp.la/about/map/",
            "schema":"http://schema.org",
            "oai_qdc":"http://worldcat.org/xmlschemas/qdc-1.0/"
        }
    }

def test_xml_to_kvp():
    kvp_output = xml2kvp.XML2kvp.xml_to_kvp(test_xml(), **test_xml_config())
    assert kvp_output == json.loads(test_kvp())
    print('xml to kvp test passed!')

def test_kvp_to_xml():
    xml_output = xml2kvp.XML2kvp.kvp_to_xml(json.loads(test_kvp()),
            serialize_xml=True,
            **test_kvp_config())
    assert xml_output == test_xml()
    print('kvp to xml test passed!')

def test_csv_to_xml():
    xml_output = xml2kvp.XML2kvp.kvp_to_xml(json.loads(test_kvp_from_csv()),
            serialize_xml=True,
            **test_csv_config())
    assert xml_output == test_xml()
    print('csv to xml test passed!')

test_xml_to_kvp()
test_kvp_to_xml()
test_csv_to_xml()
