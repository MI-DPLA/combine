import re


def test_check_for_mods_titleInfo(record, test_message="check for mods:titleInfo element"):
    titleInfo_elements = record.xml.xpath('//mods:titleInfo', namespaces=record.nsmap)
    if len(titleInfo_elements) > 0:
        return True
    else:
        return False


def test_check_dateIssued_format(record, test_message="check mods:dateIssued is YYYY-MM-DD or YYYY or YYYY-YYYY"):
    # get dateIssued elements
    dateIssued_elements = record.xml.xpath('//mods:dateIssued', namespaces=record.nsmap)

    # if found, check format 
    if len(dateIssued_elements) > 0:

        # loop through values and check
        for dateIssued in dateIssued_elements:

            # check format
            if dateIssued.text is not None:
                match = re.match(r'^([0-9]{4}-[0-9]{2}-[0-9]{2})|([0-9]{4})|([0-9]{4}-[0-9]{4})$', dateIssued.text)
            else:
                # allow None values to pass test 
                return True

            # match found, continue
            if match:
                continue
            else:
                return False

        # if all matches, return True
        return True

    # if none found, return True indicating passed test due to omission
    else:
        return True
