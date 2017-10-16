<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:mods="http://www.loc.gov/mods/v3" exclude-result-prefixes="mods">
  <xsl:output method="xml" indent="yes" omit-xml-declaration="yes"/>

  <xsl:template name="MODS" match="/">
      
    <xsl:param name="prefix">mods_</xsl:param>
    <xsl:param name="suffix">_ms</xsl:param>    
    <xsl:param name="MODSroot" select="//mods:mods"/>
    
    <!-- Wrap in MODS_fields -->
    <fields>

    <!-- Titles, with non-sorting prefixes -->
    <!-- ...specifically, this avoids catching relatedItem titles -->
    <xsl:for-each select="$MODSroot/mods:titleInfo/mods:title[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <!-- Add the parent titleInfo's 'type' attribute before the suffix if it exists -->
          <!-- This keeps the arabic, translated, and transliterated titles separate -->
          <xsl:choose>
            <xsl:when test="../@type">
              <xsl:value-of select="concat($prefix, local-name(), '_', ../@type, $suffix)"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:attribute>
        <xsl:if test="../mods:nonSort">
          <xsl:value-of select="../mods:nonSort/text()"/>
          <xsl:text> </xsl:text>
        </xsl:if>
        <xsl:value-of select="text()"/>
      </field>
      <!-- bit of a hack so it can be sorted on... -->
      <field>
        <xsl:attribute name="name">
          <!-- Add the parent titleInfo's 'type' attribute before the suffix if it exists -->
          <!-- This keeps the arabic, translated, and transliterated titles separate -->
          <xsl:choose>
            <xsl:when test="../@type">
              <xsl:value-of select="concat($prefix, local-name(), '_', ../@type, '_mlt')"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="concat($prefix, local-name(), '_mlt')"/>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:attribute>
        <xsl:if test="../mods:nonSort">
          <xsl:value-of select="../mods:nonSort/text()"/>
          <xsl:text> </xsl:text>
        </xsl:if>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Sub-title -->
    <xsl:for-each select="$MODSroot/mods:titleInfo/mods:subTitle[normalize-space(text())][1]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), '_mlt')"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Abstract -->
    <xsl:for-each select="$MODSroot/mods:abstract[normalize-space(text())]">		
      <field>
        <xsl:attribute name="name">
        	<xsl:choose>
	        	<xsl:when test="@type">			
		      		<xsl:value-of select="concat($prefix, local-name(), '_', @type, $suffix)"/>      		
			    </xsl:when>
			    <xsl:otherwise>
			    	<xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
		    	</xsl:otherwise>          
	    	</xsl:choose>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Genre (a.k.a. specific doctype) -->
    <xsl:for-each select="$MODSroot/mods:genre[normalize-space(text())]">
      <xsl:variable name="authority">
        <xsl:choose>
          <xsl:when test="@authority">
            <xsl:value-of select="concat('_', @authority)"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>_local_authority</xsl:text>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:variable>
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $authority, $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Resource Type (a.k.a. broad doctype) -->
    <xsl:for-each select="$MODSroot/mods:typeOfResource[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'resource_type', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- DOI, ISSN, ISBN, and any other typed IDs -->
    <xsl:for-each select="$MODSroot/mods:identifier[@type][normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of
            select="concat($prefix, local-name(), '_', translate(@type, ' ', '_'), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Names and Roles
    @TODO: examine if formating the names is necessary?-->
    <xsl:for-each select="$MODSroot/mods:name[mods:namePart and mods:role]">
      <xsl:variable name="role" select="mods:role/mods:roleTerm/text()"/>
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'name_', $role, $suffix)"/>
        </xsl:attribute>
        <!-- <xsl:for-each select="../../mods:namePart[@type='given']">-->
        <xsl:for-each select="mods:namePart[@type='given']">
          <xsl:value-of select="text()"/>
          <xsl:if test="string-length(text())=1">
            <xsl:text>.</xsl:text>
          </xsl:if>
          <xsl:text> </xsl:text>
        </xsl:for-each>
        <xsl:for-each select="mods:namePart[not(@type='given')]">
          <xsl:value-of select="text()"/>
          <xsl:if test="position()!=last()">
            <xsl:text> </xsl:text>
          </xsl:if>
        </xsl:for-each>
      </field>
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'reversed_name_', $role, $suffix)"/>
        </xsl:attribute>
        <xsl:for-each select="mods:namePart[not(@type='given')]">
          <xsl:value-of select="text()"/>
        </xsl:for-each>
        <xsl:for-each select="mods:namePart[@type='given']">
          <xsl:if test="position()=1">
            <xsl:text>, </xsl:text>
          </xsl:if>
          <xsl:value-of select="text()"/>
          <xsl:if test="string-length(text())=1">
            <xsl:text>.</xsl:text>
          </xsl:if>
          <xsl:if test="position()!=last()">
            <xsl:text> </xsl:text>
          </xsl:if>
        </xsl:for-each>
      </field>
    </xsl:for-each>

    <!--Description-->
    <xsl:for-each select="$MODSroot/mods:name/mods:description[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Notes with no type -->
    <xsl:for-each select="$MODSroot/mods:note[not(@type)][normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Notes -->
    <xsl:for-each select="$MODSroot/mods:note[@type][normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of
            select="concat($prefix, local-name(), '_', translate(@type, ' ', '_'), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Specific subjects -->
    <xsl:for-each select="$MODSroot/mods:subject/mods:topic[@type][normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of
            select="concat($prefix, local-name(), '_', translate(@type, ' ', '_'), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Coordinates (lat,long) -->
    <xsl:for-each
      select="$MODSroot/mods:subject/mods:cartographics/mods:coordinates[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Coordinates (lat,long) -->
    <xsl:for-each
      select="$MODSroot/mods:subject/mods:topic[../mods:cartographics/text()][normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'cartographic_topic', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Immediate children of Subjects / Keywords with displaylabel -->
    <xsl:for-each select="$MODSroot/mods:subject[@displayLabel]/*[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of
            select="concat($prefix, 'subject_', local-name(), '_', translate(../@displayLabel, ' ', '_'), $suffix)"
          />
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Immediate children of Subjects / Keywords -->
    <xsl:for-each select="$MODSroot/mods:subject[not(@displayLabel)]/*[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'subject_', local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Subjects / Keywords with displaylabel -->
    <xsl:for-each select="$MODSroot/mods:subject[@displayLabel][normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of
            select="concat($prefix, local-name(), '_', translate(@displayLabel, ' ', '_'), $suffix)"
          />
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Subjects / Keywords -->
    <xsl:for-each select="$MODSroot/mods:subject[not(@displayLabel)][normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Country -->
    <xsl:for-each select="$MODSroot/mods:country[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <xsl:for-each select="$MODSroot/mods:province[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <xsl:for-each select="$MODSroot/mods:county[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <xsl:for-each select="$MODSroot/mods:region[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <xsl:for-each select="$MODSroot/mods:city[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <xsl:for-each select="$MODSroot/mods:citySection[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Host Name (i.e. journal/newspaper name) -->
    <xsl:for-each
      select="$MODSroot/mods:relatedItem[@type='host']/mods:titleInfo/mods:title[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'host_title', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Other Formats (i.e. paper/original) -->
    <!--title-->
    <xsl:for-each
      select="$MODSroot/mods:relatedItem[@type='otherFormat']/mods:titleInfo/mods:title[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'otherFormat_title', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>
    <!--note-->
    <xsl:for-each
      select="$MODSroot/mods:relatedItem[@type='otherFormat']/mods:note[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'otherFormat_note', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>
    <!--physical location-->
    <xsl:for-each
      select="$MODSroot/mods:relatedItem[@type='otherFormat']/mods:location/mods:physicalLocation[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'otherFormat_physLocale', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Series Name (this means, e.g. a lecture series and is rarely used) -->
    <xsl:for-each
      select="$MODSroot/mods:relatedItem[@type='series']/mods:titleInfo/mods:title[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'series_title', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Volume (e.g. journal vol) -->
    <xsl:for-each
      select="$MODSroot/mods:part/mods:detail[@type='volume']/*[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'volume', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Issue (e.g. journal vol) -->
    <xsl:for-each select="$MODSroot/mods:part/mods:detail[@type='issue']/*[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'issue', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Subject Names -->
    <xsl:for-each select="$MODSroot/mods:subject/mods:name/mods:namePart/*[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'subject', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Physical Description -->
    <xsl:for-each select="$MODSroot/mods:physicalDescription[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Physical Description (note) -->
    <xsl:for-each select="$MODSroot/mods:physicalDescription/mods:note[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'physical_description_', local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Physical Description (form) -->
    <xsl:for-each
      select="$MODSroot/mods:physicalDescription/mods:form[@type][normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of
            select="concat($prefix, 'physical_description_', local-name(), '_', @type, $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Location -->
    <xsl:for-each select="$MODSroot/mods:location[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Location (physical) -->
    <xsl:for-each select="$MODSroot/mods:location/mods:physicalLocation[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Location (url) -->
    <xsl:for-each select="$MODSroot/mods:location/mods:url[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'location_', local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Place of publication -->
    <xsl:for-each
      select="$MODSroot/mods:originInfo/mods:place/mods:placeTerm[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, 'place_of_publication', $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Publisher's Name -->
    <xsl:for-each select="$MODSroot/mods:originInfo/mods:publisher[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Edition (Book) -->
    <xsl:for-each select="$MODSroot/mods:originInfo/mods:edition[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Date Issued (i.e. Journal Pub Date) -->
    <xsl:for-each select="$MODSroot/mods:originInfo/mods:dateIssued[normalize-space(text())]">
      <!-- Create a date field for Solr to facet -->
      <field>
        <xsl:attribute name="name">
          <!-- Attach the name of the 'point' attribute to the date field's name -->
          <!-- This will keep the start and end dates separate -->
          <xsl:choose>
            <xsl:when test="@point">
              <xsl:value-of select="concat($prefix, local-name(), '_', @point, '_dt')"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="concat($prefix, local-name(), '_dt')"/>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- 4-Digit Year from Key Date -->
    <xsl:for-each select="$MODSroot/mods:originInfo/mods:dateIssued[@keyDate='yes'][normalize-space(text())]">
      <!-- Create a date field for Solr to facet -->
      <field>
        <xsl:attribute name="name">mods_key_date_year</xsl:attribute>
        <xsl:value-of select="substring(text(),1,4)"/>
      </field>
    </xsl:for-each>

    <!-- Date Captured -->
    <xsl:for-each select="$MODSroot/mods:originInfo/mods:dateCaptured[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
      <xsl:if test="position() = 1">
        <!-- use the first for a sortable field -->
        <field>
          <xsl:attribute name="name">
            <xsl:value-of select="concat($prefix, local-name(), '_s')"/>
          </xsl:attribute>
          <xsl:value-of select="text()"/>
        </field>
      </xsl:if>
    </xsl:for-each>

    <!-- Date Created -->
    <xsl:for-each select="$MODSroot/mods:originInfo/mods:dateCreated[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
      <xsl:if test="position() = 1">
        <!-- use the first for a sortable field -->
        <field>
          <xsl:attribute name="name">
            <xsl:value-of select="concat($prefix, local-name(), '_s')"/>
          </xsl:attribute>
          <xsl:value-of select="text()"/>
        </field>
      </xsl:if>
    </xsl:for-each>

    <!-- Other Date -->
    <xsl:for-each select="$MODSroot/mods:originInfo/mods:dateOther[@type][normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of
            select="concat($prefix, local-name(), '_', translate(@type, ' ABCDEFGHIJKLMNOPQRSTUVWXYZ', '_abcdefghijklmnopqrstuvwxyz'), $suffix)"
          />
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
      <xsl:if test="position() = 1">
        <!-- use the first for a sortable field -->
        <field>
          <xsl:attribute name="name">
            <xsl:value-of
              select="concat($prefix, local-name(), '_', translate(@type, ' ', '_'), '_s')"/>
          </xsl:attribute>
          <xsl:value-of select="text()"/>
        </field>
      </xsl:if>
    </xsl:for-each>

    <!-- Copyright Date (is an okay substitute for Issued Date in many circumstances) -->
    <xsl:for-each select="$MODSroot/mods:originInfo/mods:copyrightDate[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Issuance (i.e. ongoing, monograph, etc. ) -->
    <xsl:for-each select="$MODSroot/mods:originInfo/mods:issuance[normalize-space(text())]">
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>

    <!-- Languague Term -->
    <xsl:for-each select="$MODSroot/mods:language/mods:languageTerm[normalize-space(text())]">      
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>  
    
    <!-- Access Condition -->
    <xsl:for-each select="$MODSroot/mods:accessCondition[normalize-space(text())]">
      <!--don't bother with empty space-->
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>
    </xsl:for-each>
    
    <!-- Extension Fields -->
    <!-- NOTE: no "mods" prefix for bibNo field, locally created -->
    <xsl:for-each select="$MODSroot/mods:extension/bibNo[normalize-space(text())]">         
      <field>
        <xsl:attribute name="name">
          <xsl:value-of select="concat($prefix, local-name(), $suffix)"/>
        </xsl:attribute>
        <xsl:value-of select="text()"/>
      </field>      
    </xsl:for-each>
      
    </fields>

  </xsl:template>

</xsl:stylesheet>