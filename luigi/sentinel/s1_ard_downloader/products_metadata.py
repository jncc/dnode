import json
import time
from lxml import etree

class ProductMetadataFetcher:
    def __init__(self, logger):
        # Setup Config from config file
        self.logger = logger

    """

    :param xml_file: Path to an xml file containing the Gemini metadata to be translated into a json blob
    :return: A JSON representation of the provided Gemini XML file
    """
    def xml_to_json(self, xml_file):
        t = etree.parse(xml_file)
        r = t.getroot()

        # Setup some common id strings
        characterString = '{%s}%s' % (r.nsmap['gco'], 'CharacterString')
        dateTimeString = '{%s}%s' % (r.nsmap['gco'], 'DateTime')
        dateString = '{%s}%s' % (r.nsmap['gco'], 'Date')
        distanceString = '{%}s%s' % (r.nsmap['gco'], 'Distance')
        decimalString = '{%s}%s' % (r.nsmap['gco'], 'Decimal')

        limitationsOnPublicAccess = ''
        useConstraints = ''

        for c in r.find('%s%s', (r.nsmap['gmd'], 'identificationInfo')).find('%s%s', (r.nsmap['gmd'], 'MD_DataIdentification')).findall('%s%s', (r.nsmap['gmd'], 'resourceConstraints')):
            if c.find('%s%s', (r.nsmap['gmd'], 'MD_LegalConstraints')) is not None:
                limitationsOnPublicAccess = c.find('%s%s', (r.nsmap['gmd'], 'MD_LegalConstraints')).find('%s%s', (r.nsmap['gmd'], 'otherConstraints')).find(characterString).text
            elif c.find('%s%s', (r.nsmap['gmd'], 'MD_Constraints')) is not None:
                useConstraints = c.find('%s%s', (r.nsmap['gmd'], 'MD_Constraints')).find('%s%s', (r.nsmap['gmd'], 'useLimitation')).find(characterString).text

        return {
            'Title': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'citation')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Citation')).find('{%s}%s' % (r.nsmap['gmd'], 'title')).find(characterString).text,
            'Abstract': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'abstract')).find(characterString).text,
            'TopicCategory': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'topicCategory')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_TopicCategoryCode')).text,
            'Keywords': [
                {
                    'Value': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'descriptiveKeywords')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_Keywords')).find('{%s}%s' % (r.nsmap['gmd'], 'keyword')).find(characterString).text,
                    'Vocab': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'descriptiveKeywords')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_Keywords')).find('{%s}%s' % (r.nsmap['gmd'], 'thesaurusName')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Citation')).find('{%s}%s' % (r.nsmap['gmd'], 'title')).find(characterString).text
                }
            ],
            'TemporalExtent': {
                'Begin': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'temporalElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_TemporalExtent')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gml'], 'TimePeriod')).find('{%s}%s' % (r.nsmap['gml'], 'beginPosition')).items()[0][1],
                'End': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'temporalElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_TemporalExtent')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gml'], 'TimePeriod')).find('{%s}%s' % (r.nsmap['gml'], 'endPosition')).items()[0][1]
            },
            'DatasetReferenceDate': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'Lineage': r.find('{%s}%s' % (r.nsmap['gmd'], 'dataQualityInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'DQ_DataQuality')).find('{%s}%s' % (r.nsmap['gmd'], 'lineage')).find('{%s}%s' % (r.nsmap['gmd'], 'LI_Lineage')).find('{%s}%s' % (r.nsmap['gmd'], 'statement')).find(characterString).text,
            'SpatialResolution': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'spatialResolution')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_Resolution')).find('{%s}%s' % (r.nsmap['gmd'], 'distance')).find(distanceString).text,
            'ResourceLocator': '', 
            'AdditionalInformationSource': '', 
            'DataFormat': r.find('{%s}%s' % (r.nsmap['gmd'], 'distributionInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_Distribution')).find('{%s}%s' % (r.nsmap['gmd'], 'distributionFormat')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_Format')).find('{%s}%s' % (r.nsmap['gmd'], 'name')).find(characterString).text,
            'ResponsibleOrganisation': {
                'Name': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'organisationName')).find(characterString).text,
                'Role': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'role')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_RoleCode')).text,
                'Email': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'electronicMailAddress')).find(characterString).text,
                'Telephone': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'phone')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Telephone')).find('{%s}%s' % (r.nsmap['gmd'], 'voice')).find(characterString).text,
                'Website': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'onlineResource')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_OnlineResource')).find('{%s}%s' % (r.nsmap['gmd'], 'linkage')).find('{%s}%s' % (r.nsmap['gmd'], 'URL')).text,
                'Address': {
                    'DeliveryPoint': r.find('{%s}%s' % (r.nsmap['gmd'],'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'deliveryPoint')).find(characterString).text,
                    'City': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'city')).find(characterString).text,
                    'PostalCode': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'postalCode')).find(characterString).text,
                    'Country': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'country')).find(characterString).text
                }
            },
            'LimitationsOnPublicAccess': limitationsOnPublicAccess,
            'UseConstraints': useConstraints,
            'Copyright': '',
            'SpatialReferenceSystem': r.find('{%s}%s' % (r.nsmap['gmd'], 'referenceSystemInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_ReferenceSystem')).find('{%s}%s' % (r.nsmap['gmd'], 'referenceSystemIdentifier')).find('{%s}%s' % (r.nsmap['gmd'], 'RS_Identifier')).find('{%s}%s' % (r.nsmap['gmd'], 'code')).find(characterString).text,
            'Extent': {
                'Value': 'urn:ogc:def:crs:EPSG::4326',
                'Vocab': 'http://www.epsg-registry.org/'
            },
            'MetadataDate': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'MetadataPointOfContact': {
                'Name': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'organisationName')).find(characterString).text,
                'Role': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'role')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_RoleCode')).text,
                'Email': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'electronicMailAddress')).find(characterString).text,
                'Telephone': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'phone')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Telephone')).find('{%s}%s' % (r.nsmap['gmd'], 'voice')).find(characterString).text,
                'Website': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'onlineResource')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_OnlineResource')).find('{%s}%s' % (r.nsmap['gmd'], 'linkage')).find('{%s}%s' % (r.nsmap['gmd'], 'URL')).text,
                'Address': {
                    'DeliveryPoint': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'deliveryPoint')).find(characterString).text,
                    'City': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'city')).find(characterString).text,
                    'PostalCode': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'postalCode')).find(characterString).text,
                    'Country': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'country')).find(characterString).text
                }
            },
            'ResourceType': r.find('{%s}%s' % (r.nsmap['gmd'], 'hierarchyLevel')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_ScopeCode')).text,
            'BoundingBox': {
                'North': float(r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'geographicElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_GeographicBoundingBox')).find('{%s}%s' % (r.nsmap['gmd'], 'northBoundLatitude')).find(decimalString).text),
                'South': float(r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'geographicElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_GeographicBoundingBox')).find('{%s}%s' % (r.nsmap['gmd'], 'southBoundLatitude')).find(decimalString).text),
                'East': float(r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'geographicElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_GeographicBoundingBox')).find('{%s}%s' % (r.nsmap['gmd'], 'eastBoundLongitude')).find(decimalString).text),
                'West': float(r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'geographicElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_GeographicBoundingBox')).find('{%s}%s' % (r.nsmap['gmd'], 'westBoundLongitude')).find(decimalString).text)
            }
        }
