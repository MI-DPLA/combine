<?xml version="1.0" encoding="UTF-8"?>
<schema xmlns="http://purl.oclc.org/dsdl/schematron" xmlns:mods="http://www.loc.gov/mods/v3">
	<ns prefix="mods" uri="http://www.loc.gov/mods/v3"/>
	<!-- Required top level Elements for all records record -->
	<pattern>
		<title>Required Elements for Each MODS record</title>
		<rule context="mods:mods">
			<assert test="mods:titleInfo">There must be a title element</assert>
			<assert test="count(mods:location/mods:url[@usage='primary'])=1">There must be a url pointing to the item</assert>
			<assert test="count(mods:location/mods:url[@access='preview'])=1">There must be a url pointing to a thumnail version of the item</assert>
			<assert test="count(mods:accessCondition[@type='use and reproduction'])=1">There must be a rights statement</assert>
		</rule>
	</pattern>
   
	<!-- Additional Requirements within Required Elements -->
	<pattern>
		<title>Subelements and Attributes used in TitleInfo</title>
		<rule context="mods:mods/mods:titleInfo">
			<assert test="*">TitleInfo must contain child title elements</assert>
		</rule>
		<rule context="mods:mods/mods:titleInfo/*">
			<assert test="normalize-space(.)">The title elements must contain text</assert>
		</rule>
	</pattern>
	
	<pattern>
		<title>Additional URL requirements</title>
		<rule context="mods:mods/mods:location/mods:url">
			<assert test="normalize-space(.)">The URL field must contain text</assert>
		</rule> 
	</pattern>
	
</schema>